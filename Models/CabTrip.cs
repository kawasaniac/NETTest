namespace NETTest.Models;

internal sealed record CabTrip(
    DateTime TpepPickupDatetimeUtc,
    DateTime TpepDropoffDatetimeUtc,
    byte PassengerCount,
    decimal TripDistance,
    string StoreAndForwardFlag,
    int PULocationId,
    int DOLocationId,
    decimal FareAmount,
    decimal TipAmount)
{
    public TripKey ToKey() => new(TpepPickupDatetimeUtc, TpepDropoffDatetimeUtc, PassengerCount);
}
