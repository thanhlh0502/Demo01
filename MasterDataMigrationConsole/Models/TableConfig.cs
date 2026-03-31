namespace MasterDataMigration.Models;

/// <summary>
/// Defines the configuration for a single table to be migrated.
/// </summary>
public class TableConfig
{
    /// <summary>
    /// The name of the source/target table (e.g. "Shipment").
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// The primary key column name (Identity column in Target).
    /// </summary>
    public string PrimaryKeyColumn { get; set; } = "ID";

    /// <summary>
    /// Column(s) used to filter by Trading Partner.
    /// - 1 column (e.g. ["CustomerID"]): matches CustID1 from TradingPartners config.
    /// - 2 columns (e.g. ["CustID1", "CustID2"]): matches the full (CustID1, CustID2) pair.
    /// </summary>
    public List<string> TradingPartnerColumns { get; set; } = new();

    /// <summary>
    /// When true AND TradingPartnerColumns has 2 columns:
    /// also includes rows where the 2nd column = 0 (wildcard, applies to all TPs of that customer).
    /// Typical for setting tables like ConfigINI.
    /// </summary>
    public bool AllowWildcardMatch { get; set; } = false;

    /// <summary>
    /// When the child table does NOT have TP columns directly (e.g., InformProc has no
    /// CustID1/CustID2), configure a join chain to reach the ancestor table that does.
    /// Supports multi-level joins (e.g., ValidationRue → ValidationLine → Validation).
    /// When set, TradingPartnerColumns should be empty.
    /// </summary>
    public TradingPartnerJoinConfig? TradingPartnerJoin { get; set; }

    /// <summary>Helper: returns true when TP filtering requires a parent table join.</summary>
    public bool UsesTpJoin => TradingPartnerJoin != null;

    /// <summary>
    /// The column used to detect changes (delta sync). Default: "ModifiedDate".
    /// Set to null or empty for tables that don't have a modified date column
    /// (e.g. Statement). In that case, ALL rows matching the TP filter will be
    /// extracted every run (full extract). The upsert logic still handles
    /// deduplication correctly via the mapping table.
    /// </summary>
    public string? UpdatedDateColumn { get; set; } = "ModifiedDate";

    /// <summary>
    /// Foreign key references to parent tables.
    /// Key = FK column name in this table, Value = parent table name.
    /// e.g. { "TruckID": "Shipment" } means FirmOrder.TruckID -> Shipment.ID
    /// </summary>
    public Dictionary<string, string> ForeignKeys { get; set; } = new();

    /// <summary>
    /// Columns to exclude from bulk copy/merge (e.g. computed columns).
    /// The PrimaryKeyColumn is always excluded from INSERT automatically.
    /// </summary>
    public List<string> ExcludeColumns { get; set; } = new();

    /// <summary>
    /// When true, the tool will detect rows that were deleted on Source
    /// (exist in mapping table but no longer in Source) and delete them from Target.
    /// Deletes run in reverse dependency order (children first, then parents).
    /// </summary>
    public bool SupportDelete { get; set; } = false;

    /// <summary>
    /// When set (non-empty), this table uses a composite business key instead of an
    /// auto-increment identity column for matching rows between Source and Target.
    /// e.g. ["CustID1", "CustID2", "PartNumber"]
    /// 
    /// Behaviour differences when this is set:
    /// - No mapping table (Map_X) is created — rows are matched directly on the business key.
    /// - Upsert uses a direct MERGE ON the business key columns.
    /// - Delete detection compares business keys directly (no mapping table).
    /// - FK columns referencing a business-key parent table don't need translation
    ///   (the values are already the same across environments).
    /// </summary>
    public List<string> BusinessKeyColumns { get; set; } = new();

    /// <summary>
    /// Helper: returns true when the table uses composite business keys instead of identity mapping.
    /// </summary>
    public bool UsesBusinessKey => BusinessKeyColumns.Count > 0;
}

/// <summary>
/// Configuration for filtering a child table by joining through one or more
/// parent tables to reach the ancestor that has the Trading Partner columns.
/// Supports single-level (InFormProc → InForm) and multi-level chains
/// (ValidationRue → ValidationLine → Validation).
/// </summary>
public class TradingPartnerJoinConfig
{
    /// <summary>
    /// Chain of joins from the child table up to the ancestor that has TP columns.
    /// Each step defines a FK on the "current" table pointing to the next parent.
    /// For single-level: 1 entry. For multi-level: 2+ entries (in order child→parent→grandparent).
    /// </summary>
    public List<JoinStep> Joins { get; set; } = new();

    /// <summary>TP filter columns on the final (ancestor) table (e.g., ["CustID1", "CustID2"]).</summary>
    public List<string> TradingPartnerColumns { get; set; } = new();

    /// <summary>Whether to include ancestor rows where the 2nd TP column = 0 (wildcard).</summary>
    public bool AllowWildcardMatch { get; set; } = false;
}

/// <summary>
/// A single join step in a TradingPartnerJoin chain.
/// </summary>
public class JoinStep
{
    /// <summary>The parent table to join to (e.g., "ValidationLine").</summary>
    public string ParentTable { get; set; } = string.Empty;

    /// <summary>FK column on the child (current) table (e.g., "ValidationLineID").</summary>
    public string ChildForeignKey { get; set; } = string.Empty;

    /// <summary>PK column on the parent table. Default "ID".</summary>
    public string ParentPrimaryKey { get; set; } = "ID";
}
