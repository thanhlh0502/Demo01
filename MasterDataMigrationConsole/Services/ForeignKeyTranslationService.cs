using Dapper;
using Microsoft.Data.SqlClient;
using MasterDataMigration.Models;
using Serilog;

namespace MasterDataMigration.Services;

/// <summary>
/// Step 3: Translates Foreign Key columns in staging tables from Source IDs to Target IDs
/// using per-table mapping tables (Map_[ParentTable]).
/// </summary>
public class ForeignKeyTranslationService
{
    private readonly MigrationSettings _settings;
    private readonly ILogger _logger;

    public ForeignKeyTranslationService(MigrationSettings settings, ILogger logger)
    {
        _settings = settings;
        _logger = logger;
    }

    /// <summary>
    /// For each FK defined in the table config, updates the staging table (cross-DB)
    /// to replace Source IDs with Target IDs from the parent's mapping table.
    /// FKs referencing business-key parent tables are skipped (values are already correct).
    /// </summary>
    public async Task TranslateAsync(
        TableConfig tableConfig,
        SqlConnection targetConnection,
        SqlTransaction targetTransaction)
    {
        if (tableConfig.ForeignKeys.Count == 0)
        {
            _logger.Information("Table {Table}: no foreign keys to translate.", tableConfig.TableName);
            return;
        }

        // Build a lookup of parent table configs to check for business key parents
        var parentLookup = _settings.Tables.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);

        var stagingTable = DbNames.StagingTable(tableConfig.TableName); // 3-part cross-DB name

        foreach (var (fkColumn, parentTable) in tableConfig.ForeignKeys)
        {
            // Skip FK translation if parent table uses business keys (values are already correct)
            if (parentLookup.TryGetValue(parentTable, out var parentConfig) && parentConfig.UsesBusinessKey)
            {
                _logger.Information("Skipping FK {FkColumn} -> {ParentTable}: parent uses business key (no ID translation needed).",
                    fkColumn, parentTable);
                continue;
            }

            var parentMapTable = DbNames.MappingTable(parentTable); // [Map_ParentTable]

            _logger.Information("Translating FK {FkColumn} -> {ParentTable} in staging...",
                fkColumn, parentTable);

            // First: remove staging rows whose parent was never migrated (FK not in parent mapping).
            // Must run BEFORE the FK translation UPDATE, because after translation
            // the FK values become Target_IDs which wouldn't match Map.Source_ID.
            var cleanupSql = $@"
                DELETE S
                FROM {stagingTable} S
                WHERE NOT EXISTS (
                    SELECT 1 FROM {parentMapTable} Map
                    WHERE S.[{fkColumn}] = Map.Source_ID
                )";

            var orphaned = await targetConnection.ExecuteAsync(
                cleanupSql,
                transaction: targetTransaction,
                commandTimeout: _settings.CommandTimeout);

            if (orphaned > 0)
                _logger.Warning("Removed {Count} orphaned rows from staging for {Table} (parent {ParentTable} not migrated for FK {FkColumn})",
                    orphaned, tableConfig.TableName, parentTable, fkColumn);

            // Then: translate FK values from Source_ID to Target_ID
            var sql = $@"
                UPDATE S
                SET S.[{fkColumn}] = Map.Target_ID
                FROM {stagingTable} S
                INNER JOIN {parentMapTable} Map 
                    ON S.[{fkColumn}] = Map.Source_ID";

            var affected = await targetConnection.ExecuteAsync(
                sql,
                transaction: targetTransaction,
                commandTimeout: _settings.CommandTimeout);

            _logger.Information("Translated {Count} rows for FK {FkColumn}", affected, fkColumn);
        }
    }
}
