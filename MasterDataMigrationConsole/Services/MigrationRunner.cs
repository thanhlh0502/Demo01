using System.Diagnostics;
using Dapper;
using Microsoft.Data.SqlClient;
using MasterDataMigration.Models;
using Serilog;

namespace MasterDataMigration.Services;

/// <summary>
/// Orchestrates the full migration workflow: iterates tables in dependency order,
/// and for each table runs Extract -> FK Translation -> Upsert -> Log.
/// </summary>
public class MigrationRunner
{
    private readonly MigrationSettings _settings;
    private readonly StagingService _stagingService;
    private readonly ForeignKeyTranslationService _fkTranslationService;
    private readonly UpsertService _upsertService;
    private readonly DeleteDetectionService _deleteService;
    private readonly ILogger _logger;

    public MigrationRunner(MigrationSettings settings, ILogger logger)
    {
        _settings = settings;
        _logger = logger;
        _stagingService = new StagingService(settings, logger);
        _fkTranslationService = new ForeignKeyTranslationService(settings, logger);
        _upsertService = new UpsertService(settings, logger);
        _deleteService = new DeleteDetectionService(settings, logger);
    }

    /// <summary>
    /// Executes one full sync run across all configured tables.
    /// </summary>
    public async Task<List<SyncRunResult>> RunAsync()
    {
        var results = new List<SyncRunResult>();
        var runStartedAt = DateTime.UtcNow;

        _logger.Information("=== Migration Sync Run started at {Time} ===", runStartedAt);
        _logger.Information("Trading Partners: {TPs}",
            string.Join(", ", _settings.TradingPartners.Select(tp => $"({tp.CustID1},{tp.CustID2})")));
        _logger.Information("Tables (in dependency order): {Tables}",
            string.Join(" -> ", _settings.Tables.Select(t => t.TableName)));

        // Ensure infrastructure tables exist on Target
        await EnsureSyncLogTableAsync();

        foreach (var tableConfig in _settings.Tables)
        {
            // Get per-table LastSyncTime from the most recent successful run for THIS table
            DateTime? lastSyncTime = await GetLastSyncTimeAsync(tableConfig.TableName);
            var result = await MigrateTableAsync(tableConfig, lastSyncTime, runStartedAt);
            results.Add(result);

            if (!result.Success)
            {
                _logger.Error("Table {Table} failed: {Error}. Continuing with next table...",
                    tableConfig.TableName, result.ErrorMessage);
            }
        }

        // --- DELETE PASS: reverse dependency order (children first, then parents) ---
        // Only runs when global EnableDelete is true AND per-table SupportDelete is true.
        var deletableTables = _settings.EnableDelete
            ? _settings.Tables.Where(t => t.SupportDelete).Reverse().ToList()
            : new List<TableConfig>();

        if (deletableTables.Count > 0)
        {
            _logger.Information("=== Delete Detection Pass (reverse order): {Tables} ===",
                string.Join(" -> ", deletableTables.Select(t => t.TableName)));

            foreach (var tableConfig in deletableTables)
            {
                var deleteResult = await DeleteFromTableAsync(tableConfig, runStartedAt);
                // Merge delete count into the existing result for this table
                var existingResult = results.FirstOrDefault(r => r.TableName == tableConfig.TableName);
                if (existingResult != null)
                {
                    existingResult.DeletedRows = deleteResult.DeletedRows;
                    if (!deleteResult.Success)
                    {
                        existingResult.Success = false;
                        existingResult.ErrorMessage = (existingResult.ErrorMessage ?? "") +
                            " | Delete error: " + deleteResult.ErrorMessage;
                    }
                }
                else
                {
                    results.Add(deleteResult);
                }
            }
        }

        _logger.Information("=== Migration Sync Run completed. {Success}/{Total} tables succeeded. ===",
            results.Count(r => r.Success), results.Count);

        return results;
    }

    private async Task<SyncRunResult> MigrateTableAsync(
        TableConfig tableConfig,
        DateTime? lastSyncTime,
        DateTime runStartedAt)
    {
        var sw = Stopwatch.StartNew();
        var result = new SyncRunResult { TableName = tableConfig.TableName };

        _logger.Information("--- Processing table: {Table} (last sync: {LastSync}) ---",
            tableConfig.TableName, lastSyncTime?.ToString("o") ?? "NEVER → full sync");

        // Ensure staging + mapping tables exist in StagingMigrate DB (SPs are on StagingMigrate)
        await using var stagingConnection = new SqlConnection(_settings.StagingConnectionString);
        await stagingConnection.OpenAsync();
        await SchemaHelper.EnsureMigrationTablesAsync(stagingConnection, tableConfig);

        // Open target connection for upsert
        await using var targetConnection = new SqlConnection(_settings.TargetConnectionString);
        await targetConnection.OpenAsync();

        // Begin staging transaction for BulkCopy
        await using var stagingTransaction = (SqlTransaction)await stagingConnection.BeginTransactionAsync();

        try
        {
            // Step 2: Extract from Source, BulkCopy into StagingMigrate DB
            result.ExtractedRows = await _stagingService.ExtractAndLoadAsync(
                tableConfig, lastSyncTime, stagingConnection, stagingTransaction);

            await stagingTransaction.CommitAsync();

            if (result.ExtractedRows == 0)
            {
                _logger.Information("No changed rows for {Table}. Skipping.", tableConfig.TableName);
                result.Success = true;
                sw.Stop();
                result.Duration = sw.Elapsed;
                await LogSyncRunAsync(runStartedAt, result);
                return result;
            }

            // Steps 3+4 run on Target connection (cross-DB reads from StagingMigrate)
            await using var targetTransaction = (SqlTransaction)await targetConnection.BeginTransactionAsync();
            try
            {
                // Step 3: FK Translation (cross-DB: staging table via 3-part name)
                await _fkTranslationService.TranslateAsync(tableConfig, targetConnection, targetTransaction);

                // Step 4: Upsert & Update Mapping
                var (inserted, updated) = await _upsertService.UpsertAsync(tableConfig, targetConnection, targetTransaction);
                result.InsertedRows = inserted;
                result.UpdatedRows = updated;

                await targetTransaction.CommitAsync();
                result.Success = true;

                _logger.Information("Table {Table}: {Extracted} extracted, {Inserted} inserted, {Updated} updated",
                    tableConfig.TableName, result.ExtractedRows, result.InsertedRows, result.UpdatedRows);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error in upsert for {Table}. Rolling back...", tableConfig.TableName);
                try { await targetTransaction.RollbackAsync(); } catch { /* best effort */ }
                result.Success = false;
                result.ErrorMessage = ex.Message;
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error staging {Table}. Rolling back...", tableConfig.TableName);
            try { await stagingTransaction.RollbackAsync(); } catch { /* best effort */ }
            result.Success = false;
            result.ErrorMessage = ex.Message;
        }

        sw.Stop();
        result.Duration = sw.Elapsed;

        // Log to sync log table (in a new connection so it persists even on failure)
        await LogSyncRunAsync(runStartedAt, result);

        return result;
    }

    private async Task<SyncRunResult> DeleteFromTableAsync(
        TableConfig tableConfig,
        DateTime runStartedAt)
    {
        var sw = Stopwatch.StartNew();
        var result = new SyncRunResult { TableName = tableConfig.TableName };

        _logger.Information("--- Delete detection: {Table} ---", tableConfig.TableName);

        await using var targetConnection = new SqlConnection(_settings.TargetConnectionString);
        await targetConnection.OpenAsync();
        await using var transaction = (SqlTransaction)await targetConnection.BeginTransactionAsync();

        try
        {
            result.DeletedRows = await _deleteService.DetectAndDeleteAsync(
                tableConfig, targetConnection, transaction);

            await transaction.CommitAsync();
            result.Success = true;
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error during delete detection for {Table}. Rolling back...", tableConfig.TableName);
            try { await transaction.RollbackAsync(); } catch { /* best effort */ }
            result.Success = false;
            result.ErrorMessage = ex.Message;
        }

        sw.Stop();
        result.Duration += sw.Elapsed;
        return result;
    }

    private async Task EnsureSyncLogTableAsync()
    {
        await using var conn = new SqlConnection(_settings.StagingConnectionString);
        await conn.OpenAsync();

        await conn.ExecuteAsync(@"
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'Migration_SyncLog') AND type = 'U')
            BEGIN
                CREATE TABLE Migration_SyncLog (
                    Id              INT IDENTITY(1,1) PRIMARY KEY,
                    RunStartedAt    DATETIME NOT NULL,
                    RunCompletedAt  DATETIME NULL,
                    TableName       VARCHAR(100) NOT NULL,
                    ExtractedRows   INT DEFAULT 0,
                    InsertedRows    INT DEFAULT 0,
                    UpdatedRows     INT DEFAULT 0,
                    DeletedRows     INT DEFAULT 0,
                    Success         BIT DEFAULT 0,
                    ErrorMessage    NVARCHAR(MAX) NULL
                );
            END");
    }

    private async Task<DateTime?> GetLastSyncTimeAsync(string tableName)
    {
        await using var conn = new SqlConnection(_settings.StagingConnectionString);
        await conn.OpenAsync();

        return await conn.QueryFirstOrDefaultAsync<DateTime?>(
            @"SELECT MAX(RunCompletedAt) FROM Migration_SyncLog WHERE Success = 1 AND TableName = @TableName",
            new { TableName = tableName });
    }

    private async Task LogSyncRunAsync(DateTime runStartedAt, SyncRunResult result)
    {
        try
        {
            await using var conn = new SqlConnection(_settings.StagingConnectionString);
            await conn.OpenAsync();

            await conn.ExecuteAsync(@"
                INSERT INTO Migration_SyncLog 
                    (RunStartedAt, RunCompletedAt, TableName, ExtractedRows, InsertedRows, UpdatedRows, DeletedRows, Success, ErrorMessage)
                VALUES 
                    (@RunStartedAt, @RunCompletedAt, @TableName, @ExtractedRows, @InsertedRows, @UpdatedRows, @DeletedRows, @Success, @ErrorMessage)",
                new
                {
                    RunStartedAt = runStartedAt,
                    RunCompletedAt = DateTime.UtcNow,
                    result.TableName,
                    result.ExtractedRows,
                    result.InsertedRows,
                    result.UpdatedRows,
                    result.DeletedRows,
                    result.Success,
                    result.ErrorMessage
                });
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to write sync log for table {Table}", result.TableName);
        }
    }
}
