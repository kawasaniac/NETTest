## Running locally

Required: .NET 10 SDK and SQL Server.

1. Install SQL Server and update `appsettings.json` with the correct `ConnectionStrings.DefaultConnection`, `SourceCsvPath`, and `DuplicatesOutputPath` (two last ones can be kept as default if using straight out of repo).
2. Keep `TruncateBeforeLoad` at `true` to fully refresh `dbo.CabTrips` on each run, and set it to `false` only if you want to append to existing data yourself.
3. Run `dotnet run --configuration Release`.

Current version of the app creates the database/table/indexes if needed, bulk-inserts in 5k batches, and reports totals when finished.
The last project run on the supplied sample csv produced 29.818 rows in `dbo.CabTrips` and wrote around 15 duplicate rows to `duplicates.csv`.

## SQL schema
- `sql/schema.sql` contains the exact layout and can be applied manually if desired.

## Queries
The schema is set up for the requested workloads too:
- 1) `IX_CabTrips_PULocation` (plus `tip_amount` include) = highest average tips per pickup location and location-filtered searches.
- 2) `IX_CabTrips_TripDistance` = top 100 longest trips by distance.
- 3) `trip_duration_minutes` + `IX_CabTrips_TripDuration` = top 100 trips by duration.

Sample query snippets are inside `sql/schema.sql`. Currently I decided not to use LINQ, because it would be an overkill for this project,
but in a production setting, we may absolutely use it, as well as other stuff like proper logging, configuration management and other DI stuff.

## Information
- `store_and_fwd_flag` -> `Yes` / `No` / `Unknown`.
- All currently existing timestamps are treated as EST and stored as UTC.
- Negative numeric values or invalid timestamps cause the row to be discarded and counted as bad rows.
- Current CSV trimming & validation are really defensive because the input should be treated as untrusted.
- Also, I decided to leave the simple-cab-data.csv file in the repo too, just to show the full pipeline that I did, but it can be easily removed and used without it.

## Scaling to ~10 GB CSV files as described in the task
Generally, for very large inputs it would be better to switch to a bit better way of streaming `IDataReader` for `SqlBulkCopy`,
replace the in-memory duplicate `HashSet` with another table, and load in chunks while temporarily disabling non-critical indexes.