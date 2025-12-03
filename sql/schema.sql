IF DB_ID(N'CabTrips') IS NULL
BEGIN
    CREATE DATABASE CabTrips;
END;
GO

USE CabTrips;
GO

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
GO

IF COL_LENGTH('dbo.CabTrips', 'trip_duration_minutes') IS NULL
BEGIN
    ALTER TABLE dbo.CabTrips
        ADD trip_duration_minutes AS (CONVERT(decimal(12,2), DATEDIFF(second, tpep_pickup_datetime, tpep_dropoff_datetime)) / 60.0) PERSISTED;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_UniqueTrip')
BEGIN
    CREATE UNIQUE INDEX IX_CabTrips_UniqueTrip ON dbo.CabTrips (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_PULocation')
BEGIN
    CREATE INDEX IX_CabTrips_PULocation ON dbo.CabTrips (PULocationID) INCLUDE (tip_amount, trip_distance, tpep_dropoff_datetime, tpep_pickup_datetime);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_TripDistance')
BEGIN
    CREATE INDEX IX_CabTrips_TripDistance ON dbo.CabTrips (trip_distance DESC);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CabTrips_TripDuration')
BEGIN
    CREATE INDEX IX_CabTrips_TripDuration ON dbo.CabTrips (trip_duration_minutes DESC);
END;
GO

-- Some analytics as requested
-- Pickup location with highest average tip
SELECT TOP 1 PULocationID, AVG(tip_amount) AS AvgTip
FROM dbo.CabTrips
GROUP BY PULocationID
ORDER BY AvgTip DESC;

-- Top 100 longest trips (distance)
SELECT TOP 100 *
FROM dbo.CabTrips
ORDER BY trip_distance DESC;

-- Top 100 longest trips (duration)
SELECT TOP 100 *
FROM dbo.CabTrips
ORDER BY trip_duration_minutes DESC;

-- Search filtered by pickup location
SELECT *
FROM dbo.CabTrips
WHERE PULocationID = 141;
