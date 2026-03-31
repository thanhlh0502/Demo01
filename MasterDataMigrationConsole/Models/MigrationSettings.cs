namespace MasterDataMigration.Models;

public class TradingPartner
{
    public long CustID1 { get; set; }
    public long CustID2 { get; set; }
}

public class MigrationSettings
{
    public string SourceConnectionString { get; set; } = string.Empty;
    public string TargetConnectionString { get; set; } = string.Empty;
    /// <summary>
    /// Connection string to the StagingMigrate database (same SQL Server instance as Target).
    /// </summary>
    public string StagingConnectionString { get; set; } = string.Empty;
    public List<TradingPartner> TradingPartners { get; set; } = new();
    public List<TableConfig> Tables { get; set; } = new();
    public int BulkCopyBatchSize { get; set; } = 10000;
    public int BulkCopyTimeout { get; set; } = 600;
    public int CommandTimeout { get; set; } = 300;

    /// <summary>
    /// Global flag to enable delete detection for ALL tables that have SupportDelete = true.
    /// When false (default), the delete pass is skipped entirely regardless of per-table settings.
    /// Typical usage: keep false during daily syncs, set true only on the final cutover run.
    /// </summary>
    public bool EnableDelete { get; set; } = false;
}
