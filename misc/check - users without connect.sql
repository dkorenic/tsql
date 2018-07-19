GO

IF OBJECT_ID('tempdb.dbo.userswithoutconnect') IS NOT NULL
BEGIN
	DROP TABLE tempdb.dbo.userswithoutconnect;
END

GO

EXEC master.dbo.sp_foreachdb @user_only = 1, @is_read_only = 0, @command1 = N'
USE [?]; 

SELECT u.name                                                                 AS principal_name
     , u.type
     , u.create_date                                                          AS CreateDate
     , u.principal_id                                                         AS ID
     , CAST(CASE dp.state WHEN N''G'' THEN 1 WHEN ''W'' THEN 1 ELSE 0 END AS bit) AS HasDBAccess
     --, u.*
INTO #trtrtr
FROM sys.database_principals                 AS u
    LEFT OUTER JOIN sys.database_permissions AS dp
        ON dp.grantee_principal_id = u.principal_id
           AND dp.type = ''CO''
WHERE (u.type IN ( ''U'', ''S'', ''G'', ''C'', ''K'', ''E'', ''X'' ))
      AND u.principal_id > 4
	  AND (dp.state NOT IN (''G'', ''W'') OR dp.state IS NULL)
ORDER BY 1 ASC;

IF @@ROWCOUNT > 0
BEGIN
	IF OBJECT_ID(''tempdb.dbo.userswithoutconnect'') IS NULL
		SELECT DB_NAME() database_name, * INTO tempdb.dbo.userswithoutconnect FROM #trtrtr 
	ELSE
		INSERT INTO tempdb.dbo.userswithoutconnect SELECT DB_NAME() database_name, * FROM #trtrtr;

	DECLARE @name sysname = '''', @sql nvarchar(MAX)
	WHILE 1=1
	BEGIN
		SELECT TOP 1 @name = principal_name FROM #trtrtr WHERE @name < principal_name ORDER BY principal_name;
		IF @@ROWCOUNT = 0 BREAK;

		SET @sql = CONCAT(''USE '', QUOTENAME(DB_NAME()), ''; GRANT CONNECT TO '', QUOTENAME(@name), '';'')
		PRINT @sql
		
		--EXEC(@sql)
	END

END
'
GO

IF OBJECT_ID('tempdb.dbo.userswithoutconnect') IS NOT NULL
	SELECT * FROM tempdb.dbo.userswithoutconnect;
