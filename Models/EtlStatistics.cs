namespace NETTest.Models;

internal sealed record EtlStatistics
{
    public int TotalRows { get; set; }
    public int InsertedRows { get; set; }
    public int DuplicateRows { get; set; }
    public int BadRows { get; set; }
}
