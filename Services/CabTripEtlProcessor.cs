using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using NETTest.Globals;
using NETTest.Helpers;
using NETTest.Mappings;
using NETTest.Models;
using System.Data;
using System.Globalization;

namespace NETTest.Services;

internal sealed class CabTripEtlProcessor(string connectionString, string csvPath, string duplicatesPath, bool truncateBeforeLoad)
{
    private const int BatchSize = 5_000;
    private const string TableName = "dbo.CabTrips";

    private readonly string _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    private readonly string _csvPath = csvPath ?? throw new ArgumentNullException(nameof(csvPath));
    private readonly string _duplicatesPath = duplicatesPath ?? throw new ArgumentNullException(nameof(duplicatesPath));
    private readonly TimeZoneInfo _sourceTimeZone = ResolveEasternTimeZone();
    private readonly bool _truncateBeforeLoad = truncateBeforeLoad;

    public async Task<EtlStatistics> RunAsync(CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_csvPath))
        {
            throw new FileNotFoundException("The source CSV file could not be found.", _csvPath);
        }

        var schema = new Schema(_connectionString);

        await schema.EnsureDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await schema.EnsureSchemaAsync(cancellationToken).ConfigureAwait(false);

        if (_truncateBeforeLoad)
        {
            await TruncateTargetTableAsync(cancellationToken).ConfigureAwait(false);
        }

        var stats = new EtlStatistics();
        var dataTable = CreateTripDataTable();

        EnsureDuplicatesDirectoryExists();

        using var duplicatesWriter = new StreamWriter(_duplicatesPath, false);
        using var duplicatesCsv = new CsvWriter(duplicatesWriter, CultureInfo.InvariantCulture);
        duplicatesCsv.Context.RegisterClassMap<CabTripCsvMap>();
        duplicatesCsv.WriteHeader<CabTrip>();
        duplicatesCsv.NextRecord();

        var seenTrips = new HashSet<TripKey>();

        using var streamReader = new StreamReader(_csvPath);
        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null,
            MissingFieldFound = null,
            HeaderValidated = null,
            DetectColumnCountChanges = false
        };

        using var csv = new CsvReader(streamReader, csvConfig);
        if (!await csv.ReadAsync().ConfigureAwait(false))
        {
            return stats;
        }

        csv.ReadHeader();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null)
        {
            DestinationTableName = TableName,
            BatchSize = BatchSize,
            BulkCopyTimeout = 0,
            EnableStreaming = true
        };

        ConfigureColumnMappings(bulkCopy, dataTable);

        while (await csv.ReadAsync().ConfigureAwait(false))
        {
            if (!TryMapTrip(csv, out var trip))
            {
                stats.BadRows++;
                continue;
            }

            stats.TotalRows++;
            if (!seenTrips.Add(trip.ToKey()))
            {
                stats.DuplicateRows++;
                duplicatesCsv.WriteRecord(trip);
                duplicatesCsv.NextRecord();
                continue;
            }

            AddTripToDataTable(trip, dataTable);
            stats.InsertedRows++;

            if (dataTable.Rows.Count >= BatchSize)
            {
                await FlushBatchAsync(bulkCopy, dataTable, cancellationToken).ConfigureAwait(false);
            }
        }

        if (dataTable.Rows.Count > 0)
        {
            await FlushBatchAsync(bulkCopy, dataTable, cancellationToken).ConfigureAwait(false);
        }

        return stats;
    }

    private async Task TruncateTargetTableAsync(CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var truncateSql = $"TRUNCATE TABLE {TableName};";
        await using var command = new SqlCommand(truncateSql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void ConfigureColumnMappings(SqlBulkCopy bulkCopy, DataTable dataTable)
    {
        bulkCopy.ColumnMappings.Add(Columns.Pickup, Columns.Pickup);
        bulkCopy.ColumnMappings.Add(Columns.Dropoff, Columns.Dropoff);
        bulkCopy.ColumnMappings.Add(Columns.PassengerCount, Columns.PassengerCount);
        bulkCopy.ColumnMappings.Add(Columns.TripDistance, Columns.TripDistance);
        bulkCopy.ColumnMappings.Add(Columns.StoreAndFwd, Columns.StoreAndFwd);
        bulkCopy.ColumnMappings.Add(Columns.PULocation, Columns.PULocation);
        bulkCopy.ColumnMappings.Add(Columns.DOLocation, Columns.DOLocation);
        bulkCopy.ColumnMappings.Add(Columns.FareAmount, Columns.FareAmount);
        bulkCopy.ColumnMappings.Add(Columns.TipAmount, Columns.TipAmount);

        foreach (SqlBulkCopyColumnMapping mapping in bulkCopy.ColumnMappings)
        {
            if (!dataTable.Columns.Contains(mapping.SourceColumn))
            {
                throw new InvalidOperationException($"DataTable is missing expected column '{mapping.SourceColumn}' required for bulk copy.");
            }
        }
    }

    private static async Task FlushBatchAsync(SqlBulkCopy bulkCopy, DataTable dataTable, CancellationToken cancellationToken)
    {
        await bulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
        dataTable.Clear();
    }

    private static DataTable CreateTripDataTable()
    {
        var table = new DataTable();
        table.Columns.Add(Columns.Pickup, typeof(DateTime));
        table.Columns.Add(Columns.Dropoff, typeof(DateTime));
        table.Columns.Add(Columns.PassengerCount, typeof(byte));
        table.Columns.Add(Columns.TripDistance, typeof(decimal));
        table.Columns.Add(Columns.StoreAndFwd, typeof(string));
        table.Columns.Add(Columns.PULocation, typeof(int));
        table.Columns.Add(Columns.DOLocation, typeof(int));
        table.Columns.Add(Columns.FareAmount, typeof(decimal));
        table.Columns.Add(Columns.TipAmount, typeof(decimal));
        return table;
    }

    private static void AddTripToDataTable(CabTrip trip, DataTable table)
    {
        table.Rows.Add(
            trip.TpepPickupDatetimeUtc,
            trip.TpepDropoffDatetimeUtc,
            trip.PassengerCount,
            trip.TripDistance,
            trip.StoreAndForwardFlag,
            trip.PULocationId,
            trip.DOLocationId,
            trip.FareAmount,
            trip.TipAmount);
    }

    private bool TryMapTrip(CsvReader csv, out CabTrip trip)
    {
        trip = default!;

        // Ideally we want to drop rows where time parsing fails or dropoff is before pickup, which are faulty data.
        if (!TryParseAndConvertDate(csv.GetField(Columns.Pickup), out var pickupUtc) ||
            !TryParseAndConvertDate(csv.GetField(Columns.Dropoff), out var dropoffUtc) ||
            dropoffUtc <= pickupUtc)
        {
            return false;
        }

        if (!TryParseByte(csv.GetField(Columns.PassengerCount), out var passengerCount) ||
            !TryParseDecimal(csv.GetField(Columns.TripDistance), out var tripDistance) ||
            !TryParseDecimal(csv.GetField(Columns.FareAmount), out var fareAmount) ||
            !TryParseDecimal(csv.GetField(Columns.TipAmount), out var tipAmount) ||
            !TryParseInt(csv.GetField(Columns.PULocation), out var puLocationId) ||
            !TryParseInt(csv.GetField(Columns.DOLocation), out var doLocationId))
        {
            return false;
        }

        if (tripDistance < 0 || fareAmount < 0 || tipAmount < 0)
        {
            return false;
        }

        var storeFlag = NormalizeStoreAndForwardFlag(csv.GetField(Columns.StoreAndFwd));

        trip = new CabTrip(
            pickupUtc,
            dropoffUtc,
            passengerCount,
            tripDistance,
            storeFlag,
            puLocationId,
            doLocationId,
            fareAmount,
            tipAmount);
        return true;
    }

    private bool TryParseAndConvertDate(string? rawValue, out DateTime utcDateTime)
    {
        utcDateTime = default;
        if (string.IsNullOrWhiteSpace(rawValue) ||
            !DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var parsed))
        {
            return false;
        }

        var unspecified = DateTime.SpecifyKind(parsed, DateTimeKind.Unspecified);
        utcDateTime = TimeZoneInfo.ConvertTimeToUtc(unspecified, _sourceTimeZone);
        return true;
    }

    private static bool TryParseByte(string? rawValue, out byte parsedValue)
    {
        parsedValue = default;
        if (!int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integer) || integer is < 0 or > byte.MaxValue)
        {
            return false;
        }

        parsedValue = (byte)integer;
        return true;
    }

    private static bool TryParseDecimal(string? rawValue, out decimal parsedValue)
    {
        return decimal.TryParse(rawValue, NumberStyles.Float, CultureInfo.InvariantCulture, out parsedValue);
    }

    private static bool TryParseInt(string? rawValue, out int parsedValue)
    {
        return int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsedValue);
    }

    private static string NormalizeStoreAndForwardFlag(string? rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return "Unknown"; // Ideally we want use 'Unknown' for empty values rather than null to keep column non-nullable
        }

        var trimmed = rawValue.Trim();
        if (string.Equals(trimmed, "Y", StringComparison.OrdinalIgnoreCase))
        {
            return "Yes";
        }

        if (string.Equals(trimmed, "N", StringComparison.OrdinalIgnoreCase))
        {
            return "No";
        }

        return trimmed;
    }

    private void EnsureDuplicatesDirectoryExists()
    {
        var directory = Path.GetDirectoryName(_duplicatesPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    // Resolve a timezone suitable for the input data. Try common IDs for Windows and Linux/macOS.
    private static TimeZoneInfo ResolveEasternTimeZone()
    {
        string[] candidates = ["Eastern Standard Time", "America/New_York"];
        foreach (var candidate in candidates)
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById(candidate);
            }
            catch (TimeZoneNotFoundException) { continue; }
            catch (InvalidTimeZoneException) { continue; }
        }

        throw new InvalidOperationException("Unable to resolve the Eastern time zone required for timestamp conversion.");
    }
}
