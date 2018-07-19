IF OBJECT_ID('tempdb.dbo.userswithwrongdefaultschema') IS NOT NULL
BEGIN
	DROP TABLE tempdb.dbo.userswithwrongdefaultschema;
END
GO

EXEC master.dbo.sp_foreachdb @user_only = 1, @is_read_only = 0, @command1 = N'
USE [?]; 
SELECT 
	name principal_name
	, type_desc
	, default_schema_name 
INTO #trtrtr
FROM 
	sys.database_principals 
WHERE 
	name != ''guest''
	AND default_schema_name != ''dbo''
ORDER BY
	principal_name;

IF @@ROWCOUNT > 0
BEGIN
	IF OBJECT_ID(''tempdb.dbo.userswithwrongdefaultschema'') IS NULL
		SELECT DB_NAME() database_name, * INTO tempdb.dbo.userswithwrongdefaultschema FROM #trtrtr 
	ELSE
		INSERT INTO tempdb.dbo.userswithwrongdefaultschema SELECT DB_NAME() database_name, * FROM #trtrtr;

	DECLARE @name sysname = '''', @sql nvarchar(MAX)
	WHILE 1=1
	BEGIN
		SELECT TOP 1 @name = principal_name FROM #trtrtr WHERE @name < principal_name ORDER BY principal_name;
		IF @@ROWCOUNT = 0 BREAK;

		SET @sql = CONCAT(''USE '', QUOTENAME(DB_NAME()), ''; ALTER USER '', QUOTENAME(@name), '' WITH DEFAULT_SCHEMA=[dbo]'')
		PRINT @sql
		
		EXEC(@sql)
		
	END
END
'
GO

IF OBJECT_ID('tempdb.dbo.userswithwrongdefaultschema') IS NOT NULL
	SELECT * FROM tempdb.dbo.userswithwrongdefaultschema;
