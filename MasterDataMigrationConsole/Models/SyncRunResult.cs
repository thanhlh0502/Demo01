namespace MasterDataMigration.Models;

public class SyncRunResult
{
    public string TableName { get; set; } = string.Empty;
    public int ExtractedRows { get; set; }
    public int InsertedRows { get; set; }
    public int UpdatedRows { get; set; }
    public int DeletedRows { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public TimeSpan Duration { get; set; }
}
