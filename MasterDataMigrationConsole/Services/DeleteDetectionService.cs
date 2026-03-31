using System.Data;
using Dapper;
using Microsoft.Data.SqlClient;
using MasterDataMigration.Models;
using Serilog;

namespace MasterDataMigration.Services;

/// <summary>
/// Detects rows that were deleted on Source (exist in mapping but no longer in Source)
/// and deletes them from Target + removes the mapping entry.
/// Uses per-table mapping tables (Map_[TableName]).
/// </summary>
public class DeleteDetectionService
{
    private readonly MigrationSettings _settings;
    private readonly ILogger _logger;

    public DeleteDetectionService(MigrationSettings settings, ILogger logger)
    {
        _settings = settings;
        _logger = logger;
    }

    /// <summary>
    /// Detects and deletes rows removed from Source.
    /// For identity-based tables: compares Source IDs against the mapping table.
    /// For business-key tables: compares business key combos directly between Source and Target.
    /// Returns the number of rows deleted.
    /// </summary>
    public async Task<int> DetectAndDeleteAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction)
    {
        if (tableConfig.UsesBusinessKey)
            return await DetectAndDeleteByBusinessKeyAsync(tableConfig, targetConnection, targetTransaction);

        return await DetectAndDeleteByIdentityAsync(tableConfig, targetConnection, targetTransaction);
    }

    private async Task<int> DetectAndDeleteByBusinessKeyAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction)
    {
        var tableName = tableConfig.TableName;
        var bizKeys = tableConfig.BusinessKeyColumns;

        _logger.Information("Delete detection (business key) for {Table}: querying Source...", tableName);

        // Step 1: Load all business key combos from Source
        var keySelectList = string.Join(", ", bizKeys.Select(k => $"[{k}]"));
        string sourceSql;
        string targetTpFilter;
        if (tableConfig.UsesTpJoin)
        {
            var joinConfig = tableConfig.TradingPartnerJoin!;
            var (joinClause, whereClause) = TpFilterBuilder.BuildJoinChain(joinConfig, _settings.TradingPartners);
            var keySelectAliased = string.Join(", ", bizKeys.Select(k => $"T.[{k}]"));
            sourceSql = $"SELECT {keySelectAliased} FROM [{tableName}] T {joinClause} WHERE {whereClause}";
            targetTpFilter = TpFilterBuilder.BuildExistsForParentJoin(joinConfig, _settings.TradingPartners);
        }
        else
        {
            targetTpFilter = TpFilterBuilder.Build(tableConfig, _settings.TradingPartners);
            sourceSql = $"SELECT {keySelectList} FROM [{tableName}] WHERE {targetTpFilter}";
        }

        List<dynamic> sourceKeys;
        await using (var sourceConn = new SqlConnection(_settings.SourceConnectionString))
        {
            await sourceConn.OpenAsync();
            sourceKeys = (await sourceConn.QueryAsync(sourceSql, commandTimeout: _settings.CommandTimeout)).ToList();
        }

        _logger.Information("Delete detection (business key) for {Table}: {Count} key combos on Source.", tableName, sourceKeys.Count);

        if (sourceKeys.Count == 0)
        {
            _logger.Warning("No rows found on Source for {Table}. Skipping delete to avoid accidental full wipe.", tableName);
            return 0;
        }

        // Step 2: Build a temp table on Target with Source keys, then DELETE mismatches
        // Create temp table with business key columns
        var tempTable = $"#Del_{tableName}";
        var tempColDefs = string.Join(", ", bizKeys.Select(k => $"[{k}] sql_variant NOT NULL"));
        await targetConnection.ExecuteAsync(
            $"CREATE TABLE {tempTable} ({tempColDefs})",
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        // Bulk insert Source keys into temp table
        var insertValues = new List<string>();
        foreach (var row in sourceKeys)
        {
            var dict = (IDictionary<string, object>)row;
            var vals = bizKeys.Select(k => dict[k] == null ? "NULL" : $"'{dict[k]}'");
            insertValues.Add($"({string.Join(", ", vals)})");

            if (insertValues.Count >= 1000)
            {
                var insertSql = $"INSERT INTO {tempTable} ({keySelectList}) VALUES {string.Join(", ", insertValues)}";
                await targetConnection.ExecuteAsync(insertSql, transaction: targetTransaction, commandTimeout: _settings.CommandTimeout);
                insertValues.Clear();
            }
        }
        if (insertValues.Count > 0)
        {
            var insertSql = $"INSERT INTO {tempTable} ({keySelectList}) VALUES {string.Join(", ", insertValues)}";
            await targetConnection.ExecuteAsync(insertSql, transaction: targetTransaction, commandTimeout: _settings.CommandTimeout);
        }

        // Step 3: Delete rows from Target that are not in the temp table (for relevant TPs)
        var joinOn = string.Join(" AND ", bizKeys.Select(k => $"T.[{k}] = Src.[{k}]"));

        var deleteSql = $@"
            DELETE T
            FROM [{tableName}] T
            WHERE NOT EXISTS (
                SELECT 1 FROM {tempTable} Src WHERE {joinOn}
            )
            AND {targetTpFilter}";

        var deleted = await targetConnection.ExecuteAsync(
            deleteSql,
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        // Also remove mapping entries for deleted rows
        var mapTable = DbNames.MappingTable(tableName);
        var mapJoinOn = string.Join(" AND ", bizKeys.Select(k => $"M.[{k}] = Src.[{k}]"));

        string deleteMapSql;
        if (tableConfig.UsesTpJoin)
        {
            // For parent-join tables the mapping table doesn't have TP columns.
            // Scoping by temp table alone is safe because only configured TPs' data is migrated.
            deleteMapSql = $@"
                DELETE M
                FROM {mapTable} M
                WHERE NOT EXISTS (
                    SELECT 1 FROM {tempTable} Src WHERE {mapJoinOn}
                )";
        }
        else
        {
            deleteMapSql = $@"
                DELETE M
                FROM {mapTable} M
                WHERE NOT EXISTS (
                    SELECT 1 FROM {tempTable} Src WHERE {mapJoinOn}
                )
                AND {targetTpFilter}";
        }

        await targetConnection.ExecuteAsync(
            deleteMapSql,
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        // Drop temp table
        await targetConnection.ExecuteAsync(
            $"DROP TABLE {tempTable}",
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        _logger.Information("Delete detection (business key) for {Table}: deleted {Count} rows from Target and mapping.", tableName, deleted);
        return deleted;
    }

    private async Task<int> DetectAndDeleteByIdentityAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction)
    {
        var tableName = tableConfig.TableName;
        var pkColumn = tableConfig.PrimaryKeyColumn;
        var mapTable = DbNames.MappingTable(tableName);

        _logger.Information("Delete detection for {Table}: querying Source for existing IDs...", tableName);

        // Step 1: Get all existing IDs from Source for the relevant TPs
        string sourceSql;
        if (tableConfig.UsesTpJoin)
        {
            var joinConfig = tableConfig.TradingPartnerJoin!;
            var (joinClause, whereClause) = TpFilterBuilder.BuildJoinChain(joinConfig, _settings.TradingPartners);
            sourceSql = $"SELECT T.[{pkColumn}] FROM [{tableName}] T {joinClause} WHERE {whereClause}";
        }
        else
        {
            var tpWhereClause = TpFilterBuilder.Build(tableConfig, _settings.TradingPartners);
            sourceSql = $"SELECT [{pkColumn}] FROM [{tableName}] WHERE {tpWhereClause}";
        }

        HashSet<long> sourceIds;
        await using (var sourceConnection = new SqlConnection(_settings.SourceConnectionString))
        {
            await sourceConnection.OpenAsync();
            var ids = await sourceConnection.QueryAsync<long>(sourceSql, commandTimeout: _settings.CommandTimeout);
            sourceIds = new HashSet<long>(ids);
        }

        _logger.Information("Delete detection for {Table}: {Count} IDs currently exist on Source.", tableName, sourceIds.Count);

        // Step 2: Get all mapped Source_IDs from per-table mapping
        var mappedRows = (await targetConnection.QueryAsync<(long SourceId, long TargetId)>(
            $"SELECT Source_ID, Target_ID FROM {mapTable}",
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout)).ToList();

        // Step 3: Find mapped rows whose Source_ID no longer exists in Source
        var deletedMappings = mappedRows.Where(m => !sourceIds.Contains(m.SourceId)).ToList();

        if (deletedMappings.Count == 0)
        {
            _logger.Information("Delete detection for {Table}: no deletions detected.", tableName);
            return 0;
        }

        _logger.Information("Delete detection for {Table}: {Count} rows to delete from Target.",
            tableName, deletedMappings.Count);

        // Step 4: Delete from Target table using Target_IDs
        var targetIdsToDelete = deletedMappings.Select(m => m.TargetId).ToList();

        int totalDeleted = 0;
        foreach (var batch in Batch(targetIdsToDelete, 2000))
        {
            var inList = string.Join(",", batch);

            var deleteFromTable = $"DELETE FROM [{tableName}] WHERE [{pkColumn}] IN ({inList})";
            totalDeleted += await targetConnection.ExecuteAsync(
                deleteFromTable,
                transaction: targetTransaction,
                commandTimeout: _settings.CommandTimeout);
        }

        // Step 5: Remove mapping entries
        var sourceIdsToRemove = deletedMappings.Select(m => m.SourceId).ToList();
        foreach (var batch in Batch(sourceIdsToRemove, 2000))
        {
            var inList = string.Join(",", batch);
            await targetConnection.ExecuteAsync(
                $"DELETE FROM {mapTable} WHERE Source_ID IN ({inList})",
                transaction: targetTransaction,
                commandTimeout: _settings.CommandTimeout);
        }

        _logger.Information("Delete detection for {Table}: deleted {Count} rows from Target and mapping.",
            tableName, totalDeleted);

        return totalDeleted;
    }

    private static IEnumerable<List<T>> Batch<T>(List<T> source, int batchSize)
    {
        for (int i = 0; i < source.Count; i += batchSize)
        {
            yield return source.GetRange(i, Math.Min(batchSize, source.Count - i));
        }
    }
}
