USE master;
GO

ALTER PROC dbo.sp_create_db_snapshots
	@useHours bit = 1,
	@useMinutes bit = 0,
	@dropOld bit = 0,
	@overwriteExisting bit = 0
AS

DECLARE @dbname sysname      = ''
      , @dbid   int
      , @suffix nvarchar(16) = FORMAT(GETDATE(), 'yyyyMMdd' + IIF(ISNULL(@useHours, 0) = 1 OR ISNULL(@useMinutes, 0) = 1, 'HH', '') + IIF(ISNULL(@useMinutes, 0) = 1, 'mm', ''));

WHILE 1 = 1
BEGIN
    SELECT TOP 1
        @dbname = name
      , @dbid   = database_id
    FROM sys.databases
    WHERE source_database_id IS NULL
          AND database_id > 4
          AND name > @dbname
    ORDER BY name;
    IF @@ROWCOUNT = 0
        BREAK;

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

    --SELECT @out
    --     , @sql;
	--PRINT @sql;
    EXEC (@sql);

    DECLARE @oldname sysname = @newname;
    WHILE
    (SELECT COUNT(1) FROM sys.databases WHERE source_database_id = @dbid AND name < @newname) > 0
    BEGIN
        SELECT TOP 1
            @oldname = name
        FROM sys.databases
        WHERE source_database_id = @dbid
              AND name < @oldname
        ORDER BY name DESC;
        IF @@ROWCOUNT = 0
            BREAK;

        SET @sql = CONCAT('DROP DATABASE ', QUOTENAME(@oldname));

		IF @dropOld = 1
		BEGIN
			PRINT CONCAT('DROPPING ', QUOTENAME(@oldname));
			EXEC (@sql);
		END
		ELSE
	        PRINT CONCAT ('-- ', @sql);
    END;
END;


GO

-- EXEC master.dbo.sp_create_db_snapshots
