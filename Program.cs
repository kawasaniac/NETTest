using Microsoft.Extensions.Configuration;
using NETTest.Services;

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .AddEnvironmentVariables()
    .Build();

var csvPath = configuration["SourceCsvPath"] ?? throw new InvalidOperationException("SourceCsvPath must be configured.");
var duplicatesPath = configuration["DuplicatesOutputPath"] ?? "duplicates.csv";
var connectionString = configuration.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("Connection string 'DefaultConnection' was not found.");

var truncateBeforeLoad = configuration.GetValue("TruncateBeforeLoad", true);

var processor = new CabTripEtlProcessor(connectionString, csvPath, duplicatesPath, truncateBeforeLoad);
var stats = await processor.RunAsync();

Console.WriteLine($"Processed rows: {stats.TotalRows}");
Console.WriteLine($"Inserted rows: {stats.InsertedRows}");
Console.WriteLine($"Duplicates removed: {stats.DuplicateRows}");
Console.WriteLine($"Rows skipped due to bad data: {stats.BadRows}");
