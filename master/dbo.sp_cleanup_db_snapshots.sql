USE master;
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE dbo.sp_cleanup_db_snapshots
    @maxAgeInHours int = 12
  , @maxSizeOnDiskInMegabytes bigint = 10240
  , @maxSpaceUsedPercent decimal(5, 2) = 1.00
  , @maxSnapshots int = 1
  , @print tinyint = 0
  , @dryRun bit = 1
AS
DECLARE @sourceDatabaseId      int = 0
      , @databaseId            int
      , @createDate            datetime2
      , @ageInHours            int
      , @sizeInMegabytes       bigint
      , @sizeOnDiskInMegabytes bigint
      , @spaceUsedPercent      decimal(5, 2)
      , @snapshotOrder         int
      --
      , @sql                   nvarchar(MAX)
      , @drop                  bit;

WHILE 1 = 1
BEGIN
    SELECT TOP 1
        @sourceDatabaseId = source_database_id
    FROM sys.databases
    WHERE @sourceDatabaseId < source_database_id
    ORDER BY source_database_id;
    IF @@ROWCOUNT = 0
        BREAK;

    IF @print > 2
        PRINT CONCAT('source DB: ', DB_NAME(@sourceDatabaseId));

    SET @databaseId = 0;
    WHILE 1 = 1
    BEGIN
        WITH ss AS (
            SELECT d.database_id
                 , d.source_database_id
                 , d.create_date
                 , DATEDIFF(MINUTE, d.create_date, GETDATE()) / 60 AS age_in_hours
                 --, mfs.file_id
                 , SUM(mfs.size_megabytes)                         AS size_megabytes
                 , SUM(vfs.size_on_disk_megabytes)                 AS size_on_disk_megabytes
            FROM sys.databases AS d
                OUTER APPLY
            (
                SELECT mf.file_id
                     , SUM(mf.size) * 8 / 1024 AS size_megabytes
                FROM sys.master_files AS mf
                WHERE mf.database_id = d.database_id
                      AND mf.type = 0
                GROUP BY mf.file_id
            )                  AS mfs
                OUTER APPLY
            (
                SELECT vfs.size_on_disk_bytes / 1024 / 1024 AS size_on_disk_megabytes
                FROM sys.dm_io_virtual_file_stats(d.database_id, mfs.file_id) AS vfs
            ) AS vfs
            WHERE d.source_database_id = @sourceDatabaseId
            GROUP BY DATEDIFF(MINUTE, d.create_date, GETDATE()) / 60
                   , d.database_id
                   , d.source_database_id
                   , d.create_date
        )
           , s AS (
            SELECT ss.database_id
                 , ss.source_database_id
                 , ss.create_date
                 , ss.age_in_hours
                 --, s.file_id
                 , ss.size_megabytes
                 , ss.size_on_disk_megabytes
                 , CAST(ROUND(ss.size_on_disk_megabytes * 100.0 / ss.size_megabytes, 2) AS decimal(5, 2)) AS space_used_percent
                 , ROW_NUMBER() OVER (PARTITION BY ss.source_database_id ORDER BY ss.create_date DESC)    AS snapshot_order
            FROM ss
        )
        SELECT TOP 1
            @databaseId            = s.database_id
          , @createDate            = s.create_date
          , @ageInHours            = s.age_in_hours
          , @sizeInMegabytes       = s.size_megabytes
          , @sizeOnDiskInMegabytes = s.size_on_disk_megabytes
          , @spaceUsedPercent      = s.space_used_percent
          , @snapshotOrder         = s.snapshot_order
        FROM s
        WHERE s.database_id > @databaseId
        ORDER BY s.database_id;
        IF @@ROWCOUNT = 0
            BREAK;

        IF @print > 1
            PRINT CONCAT(@snapshotOrder, '. ', DB_NAME(@databaseId), ', ', @sizeOnDiskInMegabytes, 'MB, ', @spaceUsedPercent, '%, ', @ageInHours, 'h old');

        SET @drop = 0;

        IF @snapshotOrder > @maxSnapshots
        BEGIN
            SET @drop = 1;
            IF @print > 0
                PRINT CONCAT('@snapshotOrder (', @snapshotOrder, ') > @maxSnapshots (', @maxSnapshots, ')');
        END;

        IF @ageInHours > @maxAgeInHours
        BEGIN
            SET @drop = 1;
            IF @print > 0
                PRINT CONCAT('@ageInHours (', @ageInHours, ') > @maxAgeInHours (', @maxAgeInHours, ')');
        END;

        IF @sizeOnDiskInMegabytes > @maxSizeOnDiskInMegabytes
        BEGIN
            SET @drop = 1;
            IF @print > 0
                PRINT CONCAT('@sizeOnDiskInMegabytes (', @sizeOnDiskInMegabytes, ') > @maxSizeOnDiskInMegabytes (', @maxSizeOnDiskInMegabytes, ')');
        END;

        IF @spaceUsedPercent > @maxSpaceUsedPercent
        BEGIN
            SET @drop = 1;
            IF @print > 0
                PRINT CONCAT('@spaceUsedPercent (', @spaceUsedPercent, ') > @maxSpaceUsedPercent (', @maxSpaceUsedPercent, ')');
        END;

        IF @drop = 1
        BEGIN
            SET @sql = CONCAT('DROP DATABASE ', QUOTENAME(DB_NAME(@databaseId)));

            IF @print > 0
                PRINT @sql;

            IF @dryRun = 0
                EXEC (@sql);
        END;

    END;
END;
GO


-- EXEC dbo.sp_cleanup_db_snapshots @print = 2, @dryRun = 1;

-- SELECT * FROM sys.dm_io_virtual_file_stats(8, 1)
-- SELECT * FROM sys.master_files WHERE database_id = 26

