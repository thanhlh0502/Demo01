using System.Data;
using Microsoft.Data.SqlClient;
using MasterDataMigration.Models;
using Serilog;

namespace MasterDataMigration.Services;

/// <summary>
/// Step 2: Extract data from Source and bulk-load into Staging tables on StagingMigrate DB.
/// </summary>
public class StagingService
{
    private readonly MigrationSettings _settings;
    private readonly ILogger _logger;

    public StagingService(MigrationSettings settings, ILogger logger)
    {
        _settings = settings;
        _logger = logger;
    }

    /// <summary>
    /// Extracts changed rows from Source for a given table and bulk-copies them
    /// into the staging table on StagingMigrate DB.
    /// Returns the number of rows loaded.
    /// </summary>
    public async Task<int> ExtractAndLoadAsync(
        TableConfig tableConfig,
        DateTime? lastSyncTime,
        SqlConnection stagingConnection,
        SqlTransaction stagingTransaction)
    {
        var stagingTableShort = DbNames.StagingTableShort(tableConfig.TableName);

        // Truncate staging table (runs on StagingMigrate DB)
        _logger.Information("Truncating staging table {StagingTable}...", stagingTableShort);
        using (var truncateCmd = new SqlCommand($"TRUNCATE TABLE [{stagingTableShort}]", stagingConnection, stagingTransaction))
        {
            truncateCmd.CommandTimeout = _settings.CommandTimeout;
            await truncateCmd.ExecuteNonQueryAsync();
        }

        // Build extract query from Source
        var extractSql = BuildExtractQuery(tableConfig, lastSyncTime);
        var hasDeltaColumn = !string.IsNullOrEmpty(tableConfig.UpdatedDateColumn);
        var syncMode = !hasDeltaColumn ? "FULL (no date column)" :
                       lastSyncTime.HasValue ? $"DELTA (since {lastSyncTime.Value:o})" : "FULL (first run)";
        _logger.Information("Extracting from Source: {Table} — {SyncMode}", tableConfig.TableName, syncMode);

        using var sourceConnection = new SqlConnection(_settings.SourceConnectionString);
        await sourceConnection.OpenAsync();

        using var reader = await ExecuteReaderAsync(sourceConnection, extractSql, tableConfig, lastSyncTime);

        // Bulk copy into staging table on StagingMigrate DB
        using var bulkCopy = new SqlBulkCopy(stagingConnection, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock, stagingTransaction)
        {
            DestinationTableName = $"[{stagingTableShort}]",
            BatchSize = _settings.BulkCopyBatchSize,
            BulkCopyTimeout = _settings.BulkCopyTimeout,
            EnableStreaming = true
        };

        // Map columns by name
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var colName = reader.GetName(i);
            bulkCopy.ColumnMappings.Add(colName, colName);
        }

        await bulkCopy.WriteToServerAsync(reader);
        var rowCount = bulkCopy.RowsCopied;

        _logger.Information("Loaded {RowCount} rows into {StagingTable}", rowCount, stagingTableShort);
        return rowCount;
    }

    private string BuildExtractQuery(TableConfig tableConfig, DateTime? lastSyncTime)
    {
        string sql;

        if (tableConfig.UsesTpJoin)
        {
            var (joinClause, whereClause) = TpFilterBuilder.BuildJoinChain(
                tableConfig.TradingPartnerJoin!, _settings.TradingPartners);
            sql = $"SELECT T.* FROM [{tableConfig.TableName}] T " +
                  $"{joinClause} " +
                  $"WHERE {whereClause}";

            if (lastSyncTime.HasValue && !string.IsNullOrEmpty(tableConfig.UpdatedDateColumn))
                sql += $" AND T.[{tableConfig.UpdatedDateColumn}] > @LastSyncTime";
        }
        else if (tableConfig.TradingPartnerColumns.Count > 0)
        {
            var whereClause = TpFilterBuilder.Build(tableConfig, _settings.TradingPartners);
            sql = $"SELECT * FROM [{tableConfig.TableName}] WHERE {whereClause}";

            if (lastSyncTime.HasValue && !string.IsNullOrEmpty(tableConfig.UpdatedDateColumn))
                sql += $" AND [{tableConfig.UpdatedDateColumn}] > @LastSyncTime";
        }
        else
        {
            // No TP filtering — extract all rows (e.g. reference/lookup tables like ValidationField)
            sql = $"SELECT * FROM [{tableConfig.TableName}]";

            if (lastSyncTime.HasValue && !string.IsNullOrEmpty(tableConfig.UpdatedDateColumn))
                sql += $" WHERE [{tableConfig.UpdatedDateColumn}] > @LastSyncTime";
        }

        return sql;
    }

    private async Task<SqlDataReader> ExecuteReaderAsync(
        SqlConnection connection,
        string sql,
        TableConfig tableConfig,
        DateTime? lastSyncTime)
    {
        var cmd = new SqlCommand(sql, connection);
        cmd.CommandTimeout = _settings.CommandTimeout;

        if (lastSyncTime.HasValue && !string.IsNullOrEmpty(tableConfig.UpdatedDateColumn))
        {
            cmd.Parameters.Add(new SqlParameter("@LastSyncTime", SqlDbType.DateTime) { Value = lastSyncTime.Value });
        }

        return await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
    }
}
