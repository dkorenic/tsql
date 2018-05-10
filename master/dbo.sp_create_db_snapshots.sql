USE master;
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO


CREATE PROC dbo.sp_create_db_snapshots
	@useHours bit = 0,
	@useMinutes bit = 0,
	@overwriteExisting bit = 0,
	@databaseLike nvarchar(130) = '%'
AS

DECLARE @dbname sysname      = ''
      , @dbid   int
      , @suffix nvarchar(16)
	  , @now datetime2;

WHILE 1 = 1
BEGIN
    SELECT TOP 1
        @dbname = name
      , @dbid   = database_id
    FROM sys.databases
    WHERE source_database_id IS NULL
          AND database_id > 4
          AND name > @dbname
		  AND name LIKE @databaseLike
    ORDER BY name;
    IF @@ROWCOUNT = 0
        BREAK;

	SET @now = GETDATE()
	SET @suffix = FORMAT(@now, 'yyyyMMdd' + IIF(ISNULL(@useHours, 0) = 1 OR ISNULL(@useMinutes, 0) = 1, 'HH', '') + IIF(ISNULL(@useMinutes, 0) = 1, 'mm', ''))

    DECLARE @newname sysname = CONCAT(@dbname, '_Snapshot_', @suffix);

    DECLARE @sql nvarchar(MAX) = CONCAT('
WITH s AS (
	SELECT name
		 , CONCAT(LEFT(physical_name, LEN(physical_name) - CHARINDEX(''\'', REVERSE(physical_name))), ''\', @newname, '_'', name, ''.ss'') file_name
	FROM ', QUOTENAME(@dbname), '.sys.database_files
	WHERE type_desc = ''ROWS''
)
SELECT @out = STUFF( (SELECT '','' + ''( NAME = N'''''' + name + '''''', FILENAME = N'''''', file_name, '''''' ) ''
               FROM s
               ORDER BY name
               FOR XML PATH(''''), TYPE).value(''.'', ''varchar(max)'')
            , 1, 1, '''')
')  ;

    DECLARE @out nvarchar(MAX);
    EXEC sys.sp_executesql @sql, N'@out nvarchar(max) OUT', @out = @out OUT;

    SET @sql = CONCAT(
	IIF(@overwriteExisting = 1, CONCAT('IF DB_ID(''', @newname, ''') IS NOT NULL 
	BEGIN
		PRINT ''DROPPING ', QUOTENAME(@newname), '...'';
		DROP DATABASE ', QUOTENAME(@newname), ';
	END
'), NULL), 
	'IF DB_ID(''', @newname, ''') IS NULL 
	BEGIN
		PRINT ''CREATING ', QUOTENAME(@newname), '...'';
		CREATE DATABASE ', QUOTENAME(@newname), ' ON ', @out, ' AS SNAPSHOT OF ', QUOTENAME(@dbname), ';
	END');

    EXEC (@sql);

END;

GO

-- EXEC master.dbo.sp_create_db_snapshots @useHours = 0, @useMinutes = 0

