USE master;
GO

IF OBJECT_DEFINITION(OBJECT_ID('dbo.sp_RebuildColumnstorePartitions')) IS NOT NULL
    DROP PROCEDURE dbo.sp_RebuildColumnstorePartitions;
GO

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
CREATE PROC dbo.sp_RebuildColumnstorePartitions
    @tableName sysname = NULL,
    @steps INT = 1,
    @dryRun BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    WHILE @steps > 0
    BEGIN
        SET @steps -= 1;

        DECLARE @partition_number INT,
                @objectId INT,
                @indexName sysname,
                @partitioned BIT,
                @rc INT = 0;

        SELECT TOP (1)
               @partition_number = t.partition_number,
               @objectId = t.object_id,
               @indexName = i.name,
               @partitioned = IIF(ps.data_space_id IS NULL, 0, 1)
        FROM sys.dm_db_column_store_row_group_physical_stats AS t
            JOIN sys.indexes i
                ON t.index_id = i.index_id
                   AND t.object_id = i.object_id
            LEFT JOIN sys.partition_schemes AS ps
                ON i.data_space_id = ps.data_space_id
            CROSS APPLY
        (
            SELECT TOP (1)
                   partition_number max_partition_number
            FROM sys.dm_db_column_store_row_group_physical_stats tm
            WHERE tm.object_id = t.object_id
            ORDER BY tm.partition_number DESC
        ) mp
        WHERE (
                  t.object_id = OBJECT_ID(@tableName)
                  OR NULLIF(LTRIM(RTRIM(@tableName)), '') IS NULL
              )
              AND
              (
                  t.state_desc <> 'COMPRESSED'
                  OR t.transition_to_compressed_state_desc <> 'INDEX_BUILD'
                  OR t.deleted_rows > 0
              )
              AND t.partition_number < mp.max_partition_number
        ORDER BY t.created_time;
        SET @rc = @@ROWCOUNT;

        IF @rc = 0
            BREAK;

        DECLARE @sql2 NVARCHAR(MAX)
            = CONCAT(
                        'ALTER INDEX ',
                        QUOTENAME(@indexName),
                        ' ON ',
                        QUOTENAME(OBJECT_SCHEMA_NAME(@objectId)),
                        '.',
                        QUOTENAME(OBJECT_NAME(@objectId)),
                        ' REBUILD'
                    );
        IF @partitioned = 1
            SET @sql2 = CONCAT(@sql2, ' PARTITION = ', @partition_number);

        PRINT @sql2;

        IF @dryRun = 0
            EXEC sys.sp_executesql @stmt = @sql2;

    END;
END;

GO

EXEC sys.sp_MS_marksystemobject N'dbo.sp_RebuildColumnstorePartitions';
GO