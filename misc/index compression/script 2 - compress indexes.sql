USE master
GO


DELETE FROM tempdb.dbo.temp_for_index_compression
WHERE database_name LIKE 'ReportServer%';


DECLARE @id            int = 0
      , @database_name sysname
      , @schema_name   sysname
      , @object_name   sysname
      , @index_name    sysname
      , @s             nvarchar(MAX);

WHILE 1 = 1
BEGIN
    SELECT TOP 1
        @id            = id
      , @database_name = database_name
      , @schema_name   = schema_name
      , @object_name   = object_name
      , @index_name    = index_name
      , @s             = CONCAT('ALTER INDEX ', QUOTENAME(index_name), ' ON ', QUOTENAME(database_name), '.', QUOTENAME(schema_name), '.', QUOTENAME(object_name), ' REBUILD WITH (ONLINE = ON, DATA_COMPRESSION = PAGE)')
    FROM tempdb.dbo.temp_for_index_compression
    WHERE data_compression = 'NONE'
          AND status != '0'
    ORDER BY status
           , page_count;
    IF @@ROWCOUNT = 0
        BREAK;

    PRINT @s;
    EXEC (@s);

    UPDATE tempdb.dbo.temp_for_index_compression
    SET status = @@ERROR
    WHERE id = @id;

    BREAK;

	WAITFOR DELAY '00:00:01'
END;

-- UPDATE  tempdb.dbo.temp_for_index_compression SET status = 0 WHERE data_compression != 'NONE'

SELECT *
     , page_count * 8 * 1024 / 1024 / 1024 AS mbytes
FROM tempdb.dbo.temp_for_index_compression
--WHERE --data_compression = 'NONE'
ORDER BY status DESC
       , IIF(status = '', 1, 1) * page_count;
