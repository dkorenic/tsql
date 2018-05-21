DECLARE @database  sysname = ''
      , @principal sysname
      , @sql       nvarchar(MAX);

WHILE 1 = 1
BEGIN
    SELECT TOP 1
        @database  = d.name
      , @principal = p.name
    FROM sys.databases                  AS d
        LEFT JOIN sys.server_principals AS p
            ON p.sid = d.owner_sid
    WHERE d.database_id > 4
          AND d.owner_sid != 0x01
          AND d.name > @database
    ORDER BY d.name;
    IF @@ROWCOUNT = 0
        BREAK;

    SET @sql = CONCAT('ALTER AUTHORIZATION ON DATABASE::', QUOTENAME(@database), ' TO sa; /* ', @principal, ' */');

    PRINT @sql;

	EXEC (@sql)
END;


--IF 1=0
SELECT d.name
     , d.owner_sid
     , p.name
FROM sys.databases                  AS d
    LEFT JOIN sys.server_principals AS p
        ON p.sid = d.owner_sid;

