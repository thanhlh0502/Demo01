namespace MasterDataMigration.Services;

/// <summary>
/// Centralizes naming conventions for staging and mapping objects.
/// </summary>
public static class DbNames
{
    /// <summary>
    /// 3-part name for staging table: [StagingMigrate].[dbo].[Stg_TableName]
    /// </summary>
    public static string StagingTable(string tableName) => $"[TL_StagingMigrate].[dbo].[Stg_{tableName}]";

    /// <summary>
    /// Short name (used within StagingMigrate context): Stg_TableName
    /// </summary>
    public static string StagingTableShort(string tableName) => $"Stg_{tableName}";

    /// <summary>
    /// 3-part name for mapping table: [TL_StagingMigrate].[dbo].[Map_TableName]
    /// </summary>
    public static string MappingTable(string tableName) => $"[TL_StagingMigrate].[dbo].[Map_{tableName}]";

    /// <summary>
    /// Short name (used within StagingMigrate context): Map_TableName
    /// </summary>
    public static string MappingTableShort(string tableName) => $"Map_{tableName}";
}
