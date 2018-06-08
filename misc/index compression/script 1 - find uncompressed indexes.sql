--DROP TABLE tempdb.dbo.temp_for_index_compression
GO

DECLARE @sql nvarchar(MAX) = N'
SELECT 
	 DB_NAME() database_name
	 , p.object_id
	 , p.index_id
	 , p.partition_number
     , OBJECT_SCHEMA_NAME(p.object_id) schema_name
     , OBJECT_NAME(p.object_id) object_name
     , i.name index_name
     , p.rows
     , p.data_compression_desc data_compression
     , ISNULL(s.page_count, 0) page_count
     , ISNULL(s.compressed_page_count, 0) compressed_page_count
     --, s.page_count * 8192 / 1024 / 1024 mbytes
     --, SUM(s.page_count * 8192 / 1024 / 1024) OVER (ORDER BY s.page_count, p.rows, i.type DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) mbytes_total
     , s.partition_count
FROM sys.partitions  AS p
    JOIN sys.indexes AS i
        ON i.object_id = p.object_id
           AND i.index_id = p.index_id
    JOIN sys.tables  AS t
        ON p.object_id = t.object_id
    CROSS APPLY
(
    SELECT COUNT(1)                     AS partition_count
         , SUM(s.page_count)            AS page_count
         , SUM(s.compressed_page_count) AS compressed_page_count
    FROM sys.dm_db_index_physical_stats(DB_ID(), p.object_id, p.index_id, DEFAULT, DEFAULT) AS s
)                    AS s
WHERE 1 = 1
      AND i.index_id > 0
	  AND p.data_compression_desc = ''NONE''
--ORDER BY s.page_count, p.rows;
';

DECLARE @s nvarchar(MAX);

IF OBJECT_ID('tempdb.dbo.temp_for_index_compression') IS NULL
BEGIN
    SET @s =
    (
        SELECT CAST((
                        SELECT CONCAT(', ', name, ' ', system_type_name, ' ', IIF(is_nullable = 1, 'NULL', 'NOT NULL'))
                        FROM sys.dm_exec_describe_first_result_set(@sql, DEFAULT, DEFAULT)
                        FOR XML PATH(''), TYPE
                    ) AS nvarchar(MAX))
    );

    SET @s = CONCAT('CREATE TABLE tempdb.dbo.temp_for_index_compression (id int IDENTITY, status nvarchar(100) NOT NULL DEFAULT('''')', @s, ');');

    PRINT @s;

    EXEC (@s);
END;

--DELETE FROM tempdb.dbo.temp_for_index_compression WHERE database_name = DB_NAME();
--INSERT INTO tempdb.dbo.temp_for_index_compression EXEC (@sql);

--SELECT * FROM tempdb.dbo.temp_for_index_compression;
--SELECT database_name, schema_name, object_name, index_name, partition_number, count(1) FROM tempdb.dbo.temp_for_index_compression GROUP BY database_name, schema_name, object_name, index_name, partition_number;


SET @s = CONCAT('
WITH d AS (
    SELECT * FROM tempdb.dbo.temp_for_index_compression WHERE database_name = DB_NAME()
),
s AS (
    ', @sql, '
)
MERGE INTO d
USING s
ON d.database_name = s.database_name COLLATE DATABASE_DEFAULT
   AND d.schema_name = s.schema_name COLLATE DATABASE_DEFAULT
   AND d.object_name = s.object_name COLLATE DATABASE_DEFAULT
   AND d.index_name = s.index_name COLLATE DATABASE_DEFAULT
   AND d.partition_number = s.partition_number
WHEN MATCHED AND d.rows != s.rows 
             OR d.data_compression != s.data_compression COLLATE DATABASE_DEFAULT
             OR d.page_count != s.page_count
             OR d.compressed_page_count != s.compressed_page_count
             OR d.partition_count != s.partition_count THEN
    UPDATE SET d.rows = s.rows
             , d.data_compression = s.data_compression
             , d.page_count = s.page_count
             , d.compressed_page_count = s.compressed_page_count
             , d.partition_count = s.partition_count
WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        database_name
      , object_id
      , index_id
      , partition_number
      , schema_name
      , object_name
      , index_name
      , rows
      , data_compression
      , page_count
      , compressed_page_count
      , partition_count
    )
    VALUES
    (database_name, object_id, index_id, partition_number, schema_name, object_name, index_name, rows, data_compression, page_count, compressed_page_count, partition_count)
WHEN NOT MATCHED BY SOURCE THEN
    DELETE
--OUTPUT Inserted.*
--     , $action
--     , Deleted.*
	 ;
');

SET @s = @s;

PRINT @s;

DECLARE @db sysname = '';
WHILE 1 = 1
BEGIN
    SELECT TOP 1
        @db = name
    FROM sys.databases
    WHERE name > @db
          AND database_id > 4
          AND name NOT LIKE 'ReportServer%'
		  --AND name = 'CasaPariurilor'
    ORDER BY name;
	IF @@ROWCOUNT = 0 BREAK;

	PRINT @db;

	DECLARE @sp sysname = @db + '.sys.sp_executesql';

	--EXEC @sp N'SELECT * FROM sys.dm_exec_describe_first_result_set(@sql, DEFAULT, DEFAULT);', N'@sql nvarchar(MAX)', @sql = @sql

	EXEC @sp @s;
END;

UPDATE tempdb.dbo.temp_for_index_compression SET status = '0' WHERE data_compression != 'NONE'

