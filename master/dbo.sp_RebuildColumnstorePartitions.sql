USE master;
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
IF OBJECT_ID('dbo.sp_RebuildColumnstorePartitions') IS NULL EXEC('CREATE PROCEDURE dbo.sp_RebuildColumnstorePartitions AS PRINT 1')
GO

ALTER PROCEDURE dbo.sp_RebuildColumnstorePartitions
    @tableName sysname = NULL
  , @steps int = 1
  , @dryRun bit = 0
AS
DECLARE @partition_number int
      , @objectId         int
      , @indexId          int;

WHILE @steps > 0
BEGIN
    SET @steps -= 1;

    SELECT TOP 1
        @partition_number = t.partition_number
      , @objectId         = t.object_id
      , @indexId          = t.index_id
    FROM sys.dm_db_column_store_row_group_physical_stats AS t
    WHERE (
              t.object_id = OBJECT_ID(@tableName)
              OR NULLIF(LTRIM(RTRIM(@tableName)), '') IS NULL
          )
          AND
          (
              t.state_desc != 'COMPRESSED'
              OR t.transition_to_compressed_state_desc != 'INDEX_BUILD'
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
    IF @@ROWCOUNT = 0
        BREAK;

    DECLARE @indexName sysname = (
                                     SELECT name FROM sys.indexes WHERE index_id = @indexId AND object_id = @objectId
                                 );

    DECLARE @partitioned bit = (
                                   SELECT COUNT(1)
                                   FROM sys.indexes               AS i
                                       JOIN sys.partition_schemes AS ps
                                           ON ps.data_space_id = i.data_space_id
                                              AND i.index_id = @indexId
                                              AND i.object_id = @objectId
                               );


    DECLARE @sql nvarchar(MAX) = CONCAT('ALTER INDEX ', QUOTENAME(@indexName), ' ON ', QUOTENAME(OBJECT_SCHEMA_NAME(@objectId)), '.', QUOTENAME(OBJECT_NAME(@objectId)), ' REBUILD');
    IF @partitioned = 1
        SET @sql = CONCAT(@sql, ' PARTITION = ', @partition_number);

    PRINT @sql;

    IF @dryRun = 0
        EXEC (@sql);

END;
GO






