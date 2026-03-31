using MasterDataMigration.Models;

namespace MasterDataMigration.Services;

/// <summary>
/// Builds the WHERE clause fragment for Trading Partner filtering.
/// Shared between StagingService (extract) and DeleteDetectionService.
/// </summary>
public static class TpFilterBuilder
{
    /// <summary>
    /// Builds a SQL WHERE clause (without the "WHERE" keyword) that filters
    /// rows by Trading Partner columns directly on the table.
    /// </summary>
    public static string Build(TableConfig tableConfig, List<TradingPartner> tradingPartners)
    {
        return BuildCore(tableConfig.TradingPartnerColumns, tableConfig.AllowWildcardMatch,
                         tableConfig.TableName, tradingPartners, null);
    }

    /// <summary>
    /// Builds the INNER JOIN chain + WHERE clause for a TradingPartnerJoin config.
    /// Returns (joinClause, whereClause) where joinClause is the full INNER JOIN chain
    /// and whereClause is the TP filter on the final ancestor alias.
    /// Example for 2-level: "INNER JOIN [ValidationLine] J1 ON T.[ValidationLineID] = J1.[ValidationLineID]
    ///   INNER JOIN [Validation] J2 ON J1.[ValidationID] = J2.[ValidationID]", "J2.[CustID1] IN (...)"
    /// </summary>
    public static (string JoinClause, string WhereClause) BuildJoinChain(
        TradingPartnerJoinConfig joinConfig, List<TradingPartner> tradingPartners)
    {
        var joins = joinConfig.Joins;
        var joinParts = new List<string>();

        for (int i = 0; i < joins.Count; i++)
        {
            var step = joins[i];
            var alias = $"J{i + 1}";
            var prevAlias = i == 0 ? "T" : $"J{i}";
            joinParts.Add($"INNER JOIN [{step.ParentTable}] {alias} ON {prevAlias}.[{step.ChildForeignKey}] = {alias}.[{step.ParentPrimaryKey}]");
        }

        var joinClause = string.Join("\n            ", joinParts);
        var finalAlias = $"J{joins.Count}";
        var whereClause = BuildCore(joinConfig.TradingPartnerColumns, joinConfig.AllowWildcardMatch,
                                     joins.Last().ParentTable, tradingPartners, finalAlias);

        return (joinClause, whereClause);
    }

    /// <summary>
    /// Returns an EXISTS sub-expression for use in a DELETE/UPDATE WHERE clause
    /// where the main table is aliased as "T". Builds a chain join through parents
    /// and applies the TP filter on the final ancestor.
    /// </summary>
    public static string BuildExistsForParentJoin(TradingPartnerJoinConfig joinConfig, List<TradingPartner> tradingPartners)
    {
        var joins = joinConfig.Joins;

        // Build inline join chain inside EXISTS subquery
        // For single-level: EXISTS (SELECT 1 FROM [Inform] J1 WHERE T.[InformID] = J1.[ID] AND J1.[CustID1]...)
        // For multi-level:  EXISTS (SELECT 1 FROM [ValidationLine] J1
        //                     INNER JOIN [Validation] J2 ON J1.[ValidationID] = J2.[ValidationID]
        //                     WHERE T.[ValidationLineID] = J1.[ValidationLineID] AND J2.[CustID1]...)
        var firstStep = joins[0];
        var parts = new List<string>();
        parts.Add($"SELECT 1 FROM [{firstStep.ParentTable}] J1");

        for (int i = 1; i < joins.Count; i++)
        {
            var step = joins[i];
            var alias = $"J{i + 1}";
            var prevAlias = $"J{i}";
            parts.Add($"INNER JOIN [{step.ParentTable}] {alias} ON {prevAlias}.[{step.ChildForeignKey}] = {alias}.[{step.ParentPrimaryKey}]");
        }

        var finalAlias = $"J{joins.Count}";
        var tpWhere = BuildCore(joinConfig.TradingPartnerColumns, joinConfig.AllowWildcardMatch,
                                joins.Last().ParentTable, tradingPartners, finalAlias);

        var linkCondition = $"T.[{firstStep.ChildForeignKey}] = J1.[{firstStep.ParentPrimaryKey}]";

        return $"EXISTS ({string.Join(" ", parts)} WHERE {linkCondition} AND {tpWhere})";
    }

    private static string BuildCore(List<string> tpColumns, bool allowWildcardMatch,
        string contextName, List<TradingPartner> tradingPartners, string? alias)
    {
        string Col(string name) => alias != null ? $"{alias}.[{name}]" : $"[{name}]";

        if (tpColumns.Count == 1)
        {
            var distinctCustId1 = tradingPartners.Select(t => t.CustID1).Distinct();
            var inList = string.Join(",", distinctCustId1);
            var clause = $"{Col(tpColumns[0])} IN ({inList})";

            if (allowWildcardMatch)
                clause = $"({clause} OR {Col(tpColumns[0])} = 0)";

            return clause;
        }

        if (tpColumns.Count == 2)
        {
            var wildcardTps = tradingPartners.Where(tp => tp.CustID2 == 0).ToList();
            var exactTps = tradingPartners.Where(tp => tp.CustID2 != 0).ToList();

            var conditions = new List<string>();

            if (exactTps.Count > 0)
            {
                var pairConditions = exactTps.Select(tp =>
                    $"({Col(tpColumns[0])} = {tp.CustID1} AND {Col(tpColumns[1])} = {tp.CustID2})");
                conditions.Add(string.Join(" OR ", pairConditions));
            }

            if (wildcardTps.Count > 0)
            {
                var wildcardCustIds = wildcardTps.Select(tp => tp.CustID1).Distinct();
                var inList = string.Join(",", wildcardCustIds);
                conditions.Add($"{Col(tpColumns[0])} IN ({inList})");
            }

            var whereClause = string.Join(" OR ", conditions);

            if (allowWildcardMatch)
            {
                var allCustId1 = tradingPartners.Select(t => t.CustID1).Distinct();
                var allList = string.Join(",", allCustId1);
                whereClause = $"({whereClause}) OR ({Col(tpColumns[0])} IN ({allList}) AND {Col(tpColumns[1])} = 0)";
            }

            return $"({whereClause})";
        }

        throw new InvalidOperationException(
            $"Table {contextName}: TradingPartnerColumns must have 1 or 2 entries, got {tpColumns.Count}.");
    }
}
