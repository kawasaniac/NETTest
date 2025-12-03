using Microsoft.Data.SqlClient;

namespace NETTest.Helpers
{
    public class Schema(string connectionString)
    {
        private readonly string _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));

        public async Task EnsureDatabaseAsync(CancellationToken cancellationToken)
        {
            var builder = new SqlConnectionStringBuilder(_connectionString);
            if (string.IsNullOrWhiteSpace(builder.InitialCatalog))
            {
                throw new InvalidOperationException("Invalid Operation: Your connection string must include an Initial Catalog/Database.");
            }

            var databaseName = builder.InitialCatalog;
            var masterBuilder = new SqlConnectionStringBuilder(builder.ConnectionString)
            {
                InitialCatalog = "master"
            };

            await using var connection = new SqlConnection(masterBuilder.ConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            var safeDatabaseName = databaseName.Replace("]", "]]", StringComparison.Ordinal);
            var createDatabaseSql = $"""
IF DB_ID(N'{safeDatabaseName}') IS NULL
BEGIN
    DECLARE @sql nvarchar(max) = N'CREATE DATABASE [{safeDatabaseName}]';
    EXEC(@sql);
END
""";
            await using var command = new SqlCommand(createDatabaseSql, connection);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }


        public async Task EnsureSchemaAsync(CancellationToken cancellationToken)
        {
            const string schemaSql = """
IF OBJECT_ID('dbo.CabTrips', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.CabTrips
    (
        tpep_pickup_datetime datetime2(0) NOT NULL,
        tpep_dropoff_datetime datetime2(0) NOT NULL,
        passenger_count tinyint NOT NULL,
        trip_distance decimal(10,2) NOT NULL,
        store_and_fwd_flag nvarchar(3) NOT NULL,
        PULocationID int NOT NULL,
        DOLocationID int NOT NULL,
        fare_amount decimal(10,2) NOT NULL,
        tip_amount decimal(10,2) NOT NULL
    );
END;

IF COL_LENGTH('dbo.CabTrips', 'trip_duration_minutes') IS NULL
BEGIN
    ALTER TABLE dbo.CabTrips
        ADD trip_duration_minutes AS (CONVERT(decimal(12,2), DATEDIFF(second, tpep_pickup_datetime, tpep_dropoff_datetime)) / 60.0) PERSISTED;
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_UniqueTrip')
BEGIN
    CREATE UNIQUE INDEX IX_CabTrips_UniqueTrip ON dbo.CabTrips (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_PULocation')
BEGIN
    CREATE INDEX IX_CabTrips_PULocation ON dbo.CabTrips (PULocationID) INCLUDE (tip_amount, trip_distance, tpep_dropoff_datetime, tpep_pickup_datetime);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_TripDistance')
BEGIN
    CREATE INDEX IX_CabTrips_TripDistance ON dbo.CabTrips (trip_distance DESC);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_TripDuration')
BEGIN
    CREATE INDEX IX_CabTrips_TripDuration ON dbo.CabTrips (trip_duration_minutes DESC);
END;
""";

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new SqlCommand(schemaSql, connection);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
