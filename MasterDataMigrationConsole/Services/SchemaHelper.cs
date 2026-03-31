using System.Data;
using Dapper;
using Microsoft.Data.SqlClient;
using MasterDataMigration.Models;

namespace MasterDataMigration.Services;

public static class SchemaHelper
{
    /// <summary>
    /// Returns column names for a table. Use the appropriate connection
    /// (staging connection for staging tables, target for target tables).
    /// </summary>
    public static async Task<List<string>> GetColumnNamesAsync(
        SqlConnection connection,
        string tableName,
        IEnumerable<string>? excludeColumns = null,
        SqlTransaction? transaction = null)
    {
        var exclude = new HashSet<string>(excludeColumns ?? Enumerable.Empty<string>(), StringComparer.OrdinalIgnoreCase);

        const string sql = @"
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_NAME = @TableName AND c.TABLE_SCHEMA = 'dbo'
            ORDER BY c.ORDINAL_POSITION";

        var allColumns = await connection.QueryAsync<string>(sql, new { TableName = tableName }, transaction: transaction);
        return allColumns.Where(c => !exclude.Contains(c)).ToList();
    }

    public static async Task<string?> GetIdentityColumnAsync(
        SqlConnection connection, string tableName, SqlTransaction? transaction = null)
    {
        const string sql = @"
            SELECT c.name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(@TableName) AND c.is_identity = 1";

        return await connection.QueryFirstOrDefaultAsync<string>(sql, new { TableName = tableName }, transaction: transaction);
    }

    /// <summary>
    /// Ensures the staging table and mapping table exist in StagingMigrate DB.
    /// SPs are deployed on StagingMigrate and reference Target DB via 3-part names.
    /// For business-key tables, creates a business-key mapping table instead of identity mapping.
    /// </summary>
    public static async Task EnsureMigrationTablesAsync(
        SqlConnection stagingConnection, TableConfig tableConfig, SqlTransaction? transaction = null)
    {
        await stagingConnection.ExecuteAsync(
            "sp_Migration_CreateStagingTable",
            new { TableName = tableConfig.TableName },
            transaction: transaction,
            commandType: CommandType.StoredProcedure,
            commandTimeout: 120);

        if (tableConfig.UsesBusinessKey)
        {
            var bizKeyCsv = string.Join(",", tableConfig.BusinessKeyColumns);
            await stagingConnection.ExecuteAsync(
                "sp_Migration_CreateBusinessKeyMappingTable",
                new { TableName = tableConfig.TableName, BusinessKeyColumns = bizKeyCsv },
                transaction: transaction,
                commandType: CommandType.StoredProcedure,
                commandTimeout: 120);
        }
        else
        {
            await stagingConnection.ExecuteAsync(
                "sp_Migration_CreateMappingTable",
                new { TableName = tableConfig.TableName },
                transaction: transaction,
                commandType: CommandType.StoredProcedure,
                commandTimeout: 120);
        }
    }
}
