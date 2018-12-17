USE master
GO


SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
ALTER PROC dbo.sp_RebuildColumnstorePartitions
    @tableName sysname = NULL
  , @steps int = 1
  , @dryRun bit = 0
AS

DECLARE @sql nvarchar(MAX) = '
    SELECT TOP 1
        @partition_number = t.partition_number
      , @objectId         = t.object_id
      , @indexId          = t.index_id
	  , @indexName		  = i.name
	  , @partitioned	  = IIF(ps.data_space_id IS NULL, 0, 1)
    FROM sys.dm_db_column_store_row_group_physical_stats AS t
		JOIN sys.indexes i 
			ON t.index_id = i.index_id 
				AND t.object_id = i.object_id
		LEFT JOIN sys.partition_schemes AS ps
            ON i.data_space_id = ps.data_space_id
    WHERE (
              t.object_id = OBJECT_ID(@tableName)
              OR NULLIF(LTRIM(RTRIM(@tableName)), '''') IS NULL
          )
          AND
          (
              t.state_desc != ''COMPRESSED''
              OR t.transition_to_compressed_state_desc != ''INDEX_BUILD''
              OR t.deleted_rows > 0
          )
    /*
          AND partition_number <
          (
              SELECT MAX(partition_number)
              FROM sys.dm_db_column_store_row_group_physical_stats
              WHERE object_id = OBJECT_ID(@tableName)
          )
		  */
    ORDER BY t.created_time;
    SET @rc = @@ROWCOUNT;';

WHILE @steps > 0
BEGIN
    SET @steps -= 1;

    DECLARE @partition_number int
          , @objectId         int
          , @indexId          int
          , @indexName        sysname
          , @partitioned      bit
          , @rc               int = 0;

	--PRINT @sql

	EXEC sys.sp_executesql @sql, N'@tableName sysname, @partition_number int OUT, @objectId int OUT, @indexId int OUT, @indexName sysname OUT, @partitioned bit OUT, @rc int OUT', @tableName, @partition_number OUT, @objectId OUT, @indexId OUT, @indexName OUT, @partitioned OUT, @rc OUT

    IF @rc = 0
        BREAK;

    DECLARE @sql2 nvarchar(MAX) = CONCAT('ALTER INDEX ', QUOTENAME(@indexName), ' ON ', QUOTENAME(OBJECT_SCHEMA_NAME(@objectId)), '.', QUOTENAME(OBJECT_NAME(@objectId)), ' REBUILD');
    IF @partitioned = 1
        SET @sql2 = CONCAT(@sql2, ' PARTITION = ', @partition_number);

    PRINT @sql2;

    IF @dryRun = 0
        EXEC (@sql2);

END;

GO

