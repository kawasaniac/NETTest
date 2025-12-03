namespace NETTest.Models;

internal readonly record struct TripKey(DateTime PickupUtc, DateTime DropoffUtc, byte PassengerCount);
