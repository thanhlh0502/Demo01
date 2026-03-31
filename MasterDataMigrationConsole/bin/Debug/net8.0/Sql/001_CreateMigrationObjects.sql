-- ============================================================
-- Migration Infrastructure Objects
-- ============================================================

-- ===========================================
-- PART A: TL_StagingMigrate Database (separate DB for staging)
-- ===========================================
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'TL_StagingMigrate')
BEGIN
    CREATE DATABASE TL_StagingMigrate;
    ALTER DATABASE TL_StagingMigrate SET RECOVERY SIMPLE;
END
GO

Use TL_StagingMigrate;

-- ===========================================
-- PART B: Sync Log Table on StagingMigrate DB
-- ===========================================

-- 1. Sync Log Table: tracks each sync run (now in StagingMigrate)
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
END
GO

-- 2. Helper SP: Create per-table mapping table (Map_[TableName]) on StagingMigrate
--    Each table gets its own mapping table with BIGINT PK for fast lookups.
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'sp_Migration_CreateMappingTable') AND type = 'P')
    DROP PROCEDURE sp_Migration_CreateMappingTable;
GO

CREATE PROCEDURE sp_Migration_CreateMappingTable
    @TableName SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @MapName SYSNAME = 'Map_' + @TableName;
    DECLARE @FullName NVARCHAR(500) = '[TL_StagingMigrate].[dbo].' + QUOTENAME(@MapName);
    DECLARE @Sql NVARCHAR(MAX);

    SET @Sql = N'IF OBJECT_ID(''' + @FullName + N''', ''U'') IS NULL
    BEGIN
        CREATE TABLE ' + @FullName + N' (
            Source_ID    BIGINT NOT NULL PRIMARY KEY,
            Target_ID    BIGINT NOT NULL,
            LastSyncedAt DATETIME DEFAULT GETDATE()
        );
        CREATE INDEX ' + QUOTENAME('IX_' + @MapName + '_Target') + N' ON ' + @FullName + N'(Target_ID);
    END';
    EXEC sp_executesql @Sql;
END
GO

-- 3. Helper SP: Create staging table dynamically in TL_StagingMigrate DB
--    Drops Identity property from source table schema.
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'sp_Migration_CreateStagingTable') AND type = 'P')
    DROP PROCEDURE sp_Migration_CreateStagingTable;
GO

-- 4. Helper SP: Create per-table mapping table for business-key tables.
--    Columns are read from the source table. VARCHAR/CHAR are converted to NVARCHAR/NCHAR.
--    Adds FirstSyncedAt + LastSyncedAt tracking columns.
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'sp_Migration_CreateBusinessKeyMappingTable') AND type = 'P')
    DROP PROCEDURE sp_Migration_CreateBusinessKeyMappingTable;
GO

CREATE PROCEDURE sp_Migration_CreateStagingTable
    @TableName SYSNAME
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StagingName SYSNAME = 'Stg_' + @TableName;
    DECLARE @FullName NVARCHAR(500) = QUOTENAME(@StagingName);
    DECLARE @Sql NVARCHAR(MAX);

    -- Drop staging table if it exists (in current DB = TL_StagingMigrate)
    IF OBJECT_ID(@StagingName, 'U') IS NOT NULL
    BEGIN
        SET @Sql = N'DROP TABLE ' + @FullName;
        EXEC sp_executesql @Sql;
    END

    -- Build column list from Target DB schema, converting identity to BIGINT
    DECLARE @ColList NVARCHAR(MAX) = N'';

    SELECT @ColList = @ColList +
        CASE WHEN @ColList = N'' THEN N'' ELSE N', ' END +
        QUOTENAME(c.COLUMN_NAME) + N' ' +
        CASE
            WHEN col.is_identity = 1 THEN N'BIGINT'
            ELSE
                c.DATA_TYPE +
                CASE
                    WHEN c.DATA_TYPE IN ('varchar','nvarchar','char','nchar','varbinary')
                        THEN N'(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(20)) END + N')'
                    WHEN c.DATA_TYPE IN ('decimal','numeric')
                        THEN N'(' + CAST(c.NUMERIC_PRECISION AS NVARCHAR(20)) + N',' + CAST(c.NUMERIC_SCALE AS NVARCHAR(20)) + N')'
                    ELSE N''
                END
        END +
        CASE WHEN c.IS_NULLABLE = 'YES' THEN N' NULL' ELSE N' NOT NULL' END
    FROM [TL_Direplacement].INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN [TL_Direplacement].sys.columns col ON col.object_id = OBJECT_ID(N'[TL_Direplacement].[dbo].' + QUOTENAME(@TableName)) AND col.name = c.COLUMN_NAME
    WHERE c.TABLE_NAME = @TableName AND c.TABLE_SCHEMA = 'dbo'
    ORDER BY c.ORDINAL_POSITION;

    IF (LEN(@ColList) = 0)
    BEGIN
        RAISERROR('No columns found for table %s. Please check if the table exists and has columns.', 16, 1, @TableName);
        RETURN;
    END

    SET @Sql = N'CREATE TABLE ' + @FullName + N' (' + @ColList + N');';
    EXEC sp_executesql @Sql;
END
GO

-- ===========================================
-- PART C: Business-Key Mapping Table SP
-- ===========================================
CREATE PROCEDURE sp_Migration_CreateBusinessKeyMappingTable
    @TableName      SYSNAME,
    @BusinessKeyColumns NVARCHAR(MAX)   -- comma-separated, e.g. 'CustID1,CustID2,PartNumber'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MapName SYSNAME = 'Map_' + @TableName;
    DECLARE @FullName NVARCHAR(500) = '[TL_StagingMigrate].[dbo].' + QUOTENAME(@MapName);

    -- Check if table already exists in StagingMigrate
    DECLARE @Exists BIT = 0;
    DECLARE @CheckSql NVARCHAR(MAX) = N'IF OBJECT_ID(''' + @FullName + N''', ''U'') IS NOT NULL SET @Exists = 1';
    EXEC sp_executesql @CheckSql, N'@Exists BIT OUTPUT', @Exists = @Exists OUTPUT;
    IF @Exists = 1 RETURN;

    -- Parse comma-separated business key columns into a temp table (no STRING_SPLIT needed)
    DECLARE @KeyCols TABLE (Pos INT IDENTITY(1,1), ColName SYSNAME);
    DECLARE @Xml XML = CAST('<x>' + REPLACE(@BusinessKeyColumns, ',', '</x><x>') + '</x>' AS XML);
    INSERT INTO @KeyCols (ColName)
    SELECT LTRIM(RTRIM(n.value('.', 'SYSNAME')))
    FROM @Xml.nodes('x') AS t(n);

    -- Build column definitions from source table; convert varchar/char -> nvarchar/nchar
    DECLARE @ColDefs NVARCHAR(MAX) = N'';

    SELECT @ColDefs = @ColDefs +
        CASE WHEN @ColDefs = N'' THEN N'' ELSE N', ' END +
        QUOTENAME(c.COLUMN_NAME) + N' ' +
        CASE
            WHEN c.DATA_TYPE IN ('varchar','char') THEN
                N'n' + c.DATA_TYPE +
                N'(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN N'MAX'
                            ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(20)) END + N')'
            WHEN c.DATA_TYPE IN ('nvarchar','nchar') THEN
                c.DATA_TYPE +
                N'(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN N'MAX'
                            ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(20)) END + N')'
            WHEN c.DATA_TYPE IN ('decimal','numeric') THEN
                c.DATA_TYPE + N'(' + CAST(c.NUMERIC_PRECISION AS NVARCHAR(20)) +
                N',' + CAST(c.NUMERIC_SCALE AS NVARCHAR(20)) + N')'
            ELSE c.DATA_TYPE
        END + N' NOT NULL'
    FROM [TL_Direplacement].INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN @KeyCols k ON k.ColName = c.COLUMN_NAME
    WHERE c.TABLE_NAME = @TableName AND c.TABLE_SCHEMA = 'dbo'
    ORDER BY c.ORDINAL_POSITION;

    -- Build PK column list (preserving config order)
    DECLARE @PkCols NVARCHAR(MAX) = N'';
    SELECT @PkCols = @PkCols +
        CASE WHEN @PkCols = N'' THEN N'' ELSE N', ' END +
        QUOTENAME(k.ColName)
    FROM @KeyCols k
    ORDER BY k.Pos;

    IF (LEN(@ColDefs) = 0 OR LEN(@PkCols) = 0)
    BEGIN
        RAISERROR('No business key columns found for table %s. Please check your configuration and source table.', 16, 1, @TableName);
        RETURN;
    END

    DECLARE @Sql NVARCHAR(MAX) = N'CREATE TABLE ' + @FullName + N' (
        ' + @ColDefs + N',
        FirstSyncedAt DATETIME NOT NULL DEFAULT GETDATE(),
        LastSyncedAt  DATETIME NOT NULL DEFAULT GETDATE(),
        CONSTRAINT ' + QUOTENAME('PK_' + @MapName) + N' PRIMARY KEY (' + @PkCols + N')
    );';
    EXEC sp_executesql @Sql;
END
GO
