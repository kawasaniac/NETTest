using CsvHelper.Configuration;
using NETTest.Models;

namespace NETTest.Mappings;

internal sealed class CabTripCsvMap : ClassMap<CabTrip>
{
    public CabTripCsvMap()
    {
        Map(m => m.TpepPickupDatetimeUtc).Name("tpep_pickup_datetime");
        Map(m => m.TpepDropoffDatetimeUtc).Name("tpep_dropoff_datetime");
        Map(m => m.PassengerCount).Name("passenger_count");
        Map(m => m.TripDistance).Name("trip_distance");
        Map(m => m.StoreAndForwardFlag).Name("store_and_fwd_flag");
        Map(m => m.PULocationId).Name("PULocationID");
        Map(m => m.DOLocationId).Name("DOLocationID");
        Map(m => m.FareAmount).Name("fare_amount");
        Map(m => m.TipAmount).Name("tip_amount");
    }
}
