using Dapper;
using Microsoft.Data.SqlClient;
using MasterDataMigration.Models;
using Serilog;

namespace MasterDataMigration.Services;

/// <summary>
/// Step 4: Performs UPSERT (INSERT new rows via MERGE + OUTPUT, UPDATE existing rows)
/// and maintains per-table mapping tables (Map_[TableName]).
/// </summary>
public class UpsertService
{
    private readonly MigrationSettings _settings;
    private readonly ILogger _logger;

    public UpsertService(MigrationSettings settings, ILogger logger)
    {
        _settings = settings;
        _logger = logger;
    }

    /// <summary>
    /// Performs the upsert operation: INSERT new records, UPDATE existing ones,
    /// and capture ID mappings via OUTPUT clause.
    /// For business-key tables, uses direct MERGE on the business key (no mapping table).
    /// Returns (insertedCount, updatedCount).
    /// </summary>
    public async Task<(int Inserted, int Updated)> UpsertAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction)
    {
        if (tableConfig.UsesBusinessKey)
            return await UpsertByBusinessKeyAsync(tableConfig, targetConnection, targetTransaction);

        return await UpsertByIdentityAsync(tableConfig, targetConnection, targetTransaction);
    }

    /// <summary>
    /// Business-key path: direct MERGE matching on composite business key columns.
    /// No mapping table is used.
    /// </summary>
    private async Task<(int Inserted, int Updated)> UpsertByBusinessKeyAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction)
    {
        var tableName = tableConfig.TableName;
        var stagingTable = DbNames.StagingTable(tableName);
        var stagingShort = DbNames.StagingTableShort(tableName);
        var bizKeys = tableConfig.BusinessKeyColumns;

        // Get all columns from staging, excluding ExcludeColumns
        List<string> allColumns;
        await using (var stagingConn = new SqlConnection(_settings.StagingConnectionString))
        {
            await stagingConn.OpenAsync();
            allColumns = await SchemaHelper.GetColumnNamesAsync(stagingConn, stagingShort, tableConfig.ExcludeColumns);
        }

        // Find and exclude the identity column (if any) from INSERT
        var identityCol = await SchemaHelper.GetIdentityColumnAsync(targetConnection, tableName, targetTransaction);
        var insertColumns = identityCol != null
            ? allColumns.Where(c => !c.Equals(identityCol, StringComparison.OrdinalIgnoreCase)).ToList()
            : allColumns;

        // UPDATE columns = all columns minus identity and business key columns
        var bizKeySet = new HashSet<string>(bizKeys, StringComparer.OrdinalIgnoreCase);
        var updateColumns = insertColumns.Where(c => !bizKeySet.Contains(c)).ToList();

        // MERGE ON clause: match on all business key columns
        var mergeOn = string.Join(" AND ", bizKeys.Select(k => $"T.[{k}] = S.[{k}]"));

        // INSERT columns/values
        var insertColList = string.Join(", ", insertColumns.Select(c => $"[{c}]"));
        var insertValList = string.Join(", ", insertColumns.Select(c => $"S.[{c}]"));

        // UPDATE SET clause
        var setClause = string.Join(",\n                    ",
            updateColumns.Select(c => $"T.[{c}] = S.[{c}]"));

        var mergeSql = $@"
            MERGE INTO [{tableName}] AS T
            USING {stagingTable} AS S
            ON {mergeOn}
            WHEN MATCHED THEN
                UPDATE SET {setClause}
            WHEN NOT MATCHED THEN
                INSERT ({insertColList})
                VALUES ({insertValList})
            OUTPUT $action AS MergeAction;";

        _logger.Information("Upserting {Table} by business key ({Keys})...",
            tableName, string.Join(", ", bizKeys));

        var actions = (await targetConnection.QueryAsync<string>(
            mergeSql,
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout)).ToList();

        var inserted = actions.Count(a => a == "INSERT");
        var updated = actions.Count(a => a == "UPDATE");

        var mapTable = DbNames.MappingTable(tableName);
        var bizKeySelectList = string.Join(", ", bizKeys.Select(k => $"S.[{k}]"));
        var bizKeyMatchOn = string.Join(" AND ", bizKeys.Select(k => $"M.[{k}] = S.[{k}]"));

        var mapMergeSql = $@"
            MERGE INTO {mapTable} AS M
            USING {stagingTable} AS S
            ON {bizKeyMatchOn}
            WHEN MATCHED THEN
                UPDATE SET M.LastSyncedAt = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT ({string.Join(", ", bizKeys.Select(k => $"[{k}]"))}, FirstSyncedAt, LastSyncedAt)
                VALUES ({bizKeySelectList}, GETDATE(), GETDATE());";

        await targetConnection.ExecuteAsync(
            mapMergeSql,
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        _logger.Information("Table {Table} (business key): {Inserted} inserted, {Updated} updated",
            tableName, inserted, updated);

        return (inserted, updated);
    }

    /// <summary>
    /// Identity-based path (original): uses mapping table for ID translation.
    /// </summary>
    private async Task<(int Inserted, int Updated)> UpsertByIdentityAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction)
    {
        var tableName = tableConfig.TableName;
        var pkColumn = tableConfig.PrimaryKeyColumn;
        var stagingTable = DbNames.StagingTable(tableName);       // 3-part cross-DB
        var mapTable = DbNames.MappingTable(tableName);           // [Map_TableName]
        var stagingShort = DbNames.StagingTableShort(tableName);  // for column query

        // Get columns from staging table (need staging connection for INFORMATION_SCHEMA)
        List<string> insertColumns;
        await using (var stagingConn = new SqlConnection(_settings.StagingConnectionString))
        {
            await stagingConn.OpenAsync();
            var allColumns = await SchemaHelper.GetColumnNamesAsync(stagingConn, stagingShort, tableConfig.ExcludeColumns);

            // Only exclude the ACTUAL identity column (not just the configured PK)
            var identityCol = await SchemaHelper.GetIdentityColumnAsync(targetConnection, tableName, targetTransaction);
            if (identityCol != null)
            {
                insertColumns = allColumns.Where(c => !c.Equals(identityCol, StringComparison.OrdinalIgnoreCase)).ToList();
            }
            else
            {
                // PK is not identity — include all columns (PK value comes from Source)
                insertColumns = allColumns;
            }
        }

        // --- PART A: INSERT new rows (those not yet in mapping) via MERGE + OUTPUT ---
        var inserted = await InsertNewRowsAsync(tableConfig, targetConnection, targetTransaction, insertColumns, stagingTable, mapTable);

        // --- PART B: UPDATE existing rows (those already in mapping) ---
        var updated = await UpdateExistingRowsAsync(tableConfig, targetConnection, targetTransaction, insertColumns, stagingTable, mapTable);

        return (inserted, updated);
    }

    private async Task<int> InsertNewRowsAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction,
        List<string> insertColumns,
        string stagingTable,
        string mapTable)
    {
        var tableName = tableConfig.TableName;
        var pkColumn = tableConfig.PrimaryKeyColumn;

        var sourceColumnList = string.Join(", ", insertColumns.Select(c => $"Source.[{c}]"));
        var targetColumnList = string.Join(", ", insertColumns.Select(c => $"[{c}]"));

        // MERGE pattern: force INSERT for rows not yet mapped, capture new IDs via OUTPUT
        var mergeSql = $@"
            INSERT INTO {mapTable} (Source_ID, Target_ID, LastSyncedAt)
            SELECT Old_ID, New_ID, GETDATE()
            FROM (
                MERGE INTO [{tableName}] AS T
                USING (
                    SELECT S.*
                    FROM {stagingTable} S
                    LEFT JOIN {mapTable} M ON S.[{pkColumn}] = M.Source_ID
                    WHERE M.Target_ID IS NULL
                ) AS Source
                ON 1 = 0
                WHEN NOT MATCHED THEN
                    INSERT ({targetColumnList})
                    VALUES ({sourceColumnList})
                OUTPUT Source.[{pkColumn}] AS Old_ID, INSERTED.[{pkColumn}] AS New_ID
            ) AS MergeOutput (Old_ID, New_ID);";

        _logger.Information("Inserting new rows for {Table} via MERGE + OUTPUT...", tableName);

        var inserted = await targetConnection.ExecuteAsync(
            mergeSql,
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        _logger.Information("Inserted {Count} new rows into {Table}", inserted, tableName);
        return inserted;
    }

    private async Task<int> UpdateExistingRowsAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction,
        List<string> insertColumns,
        string stagingTable,
        string mapTable)
    {
        var tableName = tableConfig.TableName;
        var pkColumn = tableConfig.PrimaryKeyColumn;

        // Build SET clause: T.Col1 = S.Col1, T.Col2 = S.Col2, ...
        var setClause = string.Join(",\n                ",
            insertColumns.Select(c => $"T.[{c}] = S.[{c}]"));

        var updateSql = $@"
            UPDATE T
            SET {setClause}
            FROM [{tableName}] T
            INNER JOIN {mapTable} M ON T.[{pkColumn}] = M.Target_ID
            INNER JOIN {stagingTable} S ON M.Source_ID = S.[{pkColumn}];";

        _logger.Information("Updating existing rows for {Table}...", tableName);

        var updated = await targetConnection.ExecuteAsync(
            updateSql,
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        // Update LastSyncedAt in mapping for touched rows
        var updateMappingSql = $@"
            UPDATE M
            SET M.LastSyncedAt = GETDATE()
            FROM {mapTable} M
            INNER JOIN {stagingTable} S ON M.Source_ID = S.[{pkColumn}];";

        await targetConnection.ExecuteAsync(
            updateMappingSql,
            transaction: targetTransaction,
            commandTimeout: _settings.CommandTimeout);

        _logger.Information("Updated {Count} existing rows in {Table}", updated, tableName);
        return updated;
    }
}
