USE [master]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID('dbo.sp_spaceused8') IS NULL EXEC('CREATE PROCEDURE dbo.sp_spaceused8 AS PRINT 1')
GO

ALTER PROCEDURE dbo.sp_spaceused8
    @DriveLetters nvarchar(100) = NULL
  , @Databases nvarchar(100) = NULL
  , @ReportType tinyint = 2
AS
/*
2017-07-18 created by TuljanWorks - Jira task: no jira, no task ...

EXEC dbo.sp_spaceused8 @DriveLetters = 'CDE', @Databases = 'master'
EXEC dbo.sp_spaceused8 @Databases = 'clientblue'
EXEC dbo.sp_spaceused8 @reporttype=2 , @Databases = NULL
*/


DECLARE @id  int           = -1
      , @sql nvarchar(MAX) = ''
      , @db  sysname;

IF LEN(@Databases) = 0
    SET @Databases = NULL;
IF LEN(@DriveLetters) = 0
    SET @DriveLetters = NULL;
IF @ReportType IS NULL
   OR @ReportType NOT
   BETWEEN 1 AND 3
    SET @ReportType = 1;

IF OBJECT_ID('tempdb..#filesList') IS NOT NULL
    DROP TABLE #filesList;
CREATE TABLE #filesList
(
    DId int
  , DBName nvarchar(100)
  , fileid smallint
  , GroupName sysname
  , File_TotalMB decimal(12, 2)
  , File_UsedMB decimal(12, 2)
  , File_FreeMB decimal(12, 2)
);

SELECT name
     , database_id
INTO #db
FROM sys.databases WITH (NOLOCK);

WHILE 1 = 1
BEGIN
    SELECT TOP 1
        @db = name
      , @id = database_id
    FROM #db WITH (NOLOCK)
    WHERE database_id > @id
    ORDER BY database_id;

    IF @@ROWCOUNT = 0
        BREAK;

    SET @sql = CONCAT(CAST(N'' AS nvarchar(MAX)), '
USE [', @db, ']
SELECT  ''', @id, ''' As DId,
		''', @db, ''' As DBName,
		[fileid] ,
		ISNULL(fg.groupname, ''(log)'') AS GroupName,
        CONVERT(DECIMAL(12, 2), ROUND([size] / 128.000, 2)) AS [File_TotalMB] ,
        CONVERT(DECIMAL(12, 2), ROUND(FILEPROPERTY([name], ''SpaceUsed'') / 128.000, 2)) AS [File_UsedMB] ,
        CONVERT(DECIMAL(12, 2), ROUND(( [size] - FILEPROPERTY([name], ''SpaceUsed'') ) / 128.000, 2)) AS [File_FreeMB]
FROM    [sys].[sysfiles] f WITH (NOLOCK)
LEFT JOIN	sysfilegroups fg WITH (NOLOCK) ON f.groupid = fg.groupid
')  ;

    INSERT INTO #filesList
    EXECUTE (@sql);

END;

SELECT mf.database_id
     , mf.file_id
     , mf.name
     , mf.type_desc
     , dovs.volume_mount_point
     , dovs.logical_volume_name
     , dovs.total_bytes
     , dovs.available_bytes
     , mf.physical_name
INTO #masterFilesList
FROM sys.master_files                                              AS mf WITH (NOLOCK)
    CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS dovs; --WITH (NOLOCK)

IF @ReportType = 1
BEGIN
    SELECT UPPER(LEFT(mf.volume_mount_point, 1))                                                                                                                                     AS DriveLetter
         --mf.logical_volume_name AS DriveLogicalName ,
         , fl.DBName
         , mf.name                                                                                                                                                                   AS FileName
         --mf.type_desc FileType,
         , fl.GroupName
         , CONVERT(decimal(12, 2), mf.total_bytes / 1048576.0)                                                                                                                       AS Disk_TotalSpace_MB
         , CONVERT(decimal(12, 2), (mf.total_bytes - mf.available_bytes) / 1048576.0)                                                                                                AS Disk_UsedSpace_MB
         , CONVERT(decimal(5, 2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.)                                                                              AS Disk_UsedSpace_Perc
         , CONVERT(decimal(12, 2), mf.available_bytes / 1048576.0)                                                                                                                   AS Disk_FreeSpace_MB
         , ISNULL(fl.File_TotalMB, 0)                                                                                                                                                AS File_TotalMB
         , ISNULL(fl.File_UsedMB, 0)                                                                                                                                                 AS File_UsedMB
         , CONVERT(decimal(5, 2), ISNULL(fl.File_UsedMB, 0) / ISNULL(IIF(fl.File_TotalMB = 0, 1, fl.File_TotalMB), 1) * 100.)                                                        AS File_UsedPerc
         , ISNULL(fl.File_FreeMB, 0)                                                                                                                                                 AS File_FreeMB
         , CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(decimal(12, 0), ISNULL(fl.File_UsedMB, 0)) + 2000, ')   -- USE [', DB_NAME(mf.database_id), ']')                        AS ShrinkMeBaby1MoreTime
         , CONCAT('ALTER DATABASE [', DB_NAME(mf.database_id), '] MODIFY FILE (NAME = ''', mf.name, ''', SIZE = ', CEILING(fl.File_TotalMB * 1.5 / 1024), 'GB, FILEGROWTH = 1GB );') AS ExpandMeInAdvance
         , mf.physical_name                                                                                                                                                          AS FileFullPath
    FROM #masterFilesList    AS mf
        LEFT JOIN #filesList AS fl
            ON fl.DId = mf.database_id
               AND fl.fileid = mf.file_id
    WHERE ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
          AND
          (
              ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0
              OR fl.DBName LIKE CONCAT('%', @Databases, '%')
          )
    ORDER BY DriveLetter ASC
           , fl.DBName ASC;

    SELECT UPPER(LEFT(mf.volume_mount_point, 1))                                                                         AS DriveLetter
         --mf.logical_volume_name AS DriveLogicalName ,
         , fl.DBName
         --mf.name [FileName], 
         --mf.type_desc FileType,
         --fl.GroupName,
         , MAX(CONVERT(decimal(12, 2), mf.total_bytes / 1048576.0))                                                      AS Disk_TotalSpace_MB
         , MAX(CONVERT(decimal(12, 2), (mf.total_bytes - mf.available_bytes) / 1048576.0))                               AS Disk_UsedSpace_MB
         , CONVERT(decimal(5, 2), (((SUM(mf.total_bytes) - SUM(mf.available_bytes)) - 1.) / SUM(mf.total_bytes)) * 100.) AS Disk_UsedSpace_Perc
         , MAX(CONVERT(decimal(12, 2), mf.available_bytes / 1048576.0))                                                  AS Disk_FreeSpace_MB
         , SUM(ISNULL(fl.File_TotalMB, 0))                                                                               AS File_TotalMB
         , SUM(ISNULL(fl.File_UsedMB, 0))                                                                                AS File_UsedMB
         , SUM(ISNULL(fl.File_FreeMB, 0))                                                                                AS File_FreeMB
    --mf.physical_name FileFullPath
    FROM #masterFilesList    AS mf
        LEFT JOIN #filesList AS fl
            ON fl.DId = mf.database_id
               AND fl.fileid = mf.file_id
    WHERE ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
          AND
          (
              ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0
              OR fl.DBName LIKE CONCAT('%', @Databases, '%')
          )
    GROUP BY fl.DBName
           , UPPER(LEFT(mf.volume_mount_point, 1))
           , mf.logical_volume_name
    ORDER BY DriveLetter ASC
           , fl.DBName ASC;

END;
ELSE
BEGIN

    SELECT --a.DetailType ,
        a.[Disk/DB/File]
      --a.DriveLetter ,
      --a.DBName ,
      --a.Disk_TotalSpace_MB ,
      --a.Disk_UsedSpace_MB ,
      --a.Disk_UsedSpace_Perc ,
      --a.Disk_FreeSpace_MB ,
      , FORMAT(CAST(a.File_TotalMB AS decimal(12, 0)), '#,##0') AS TotalMB
      , FORMAT(CAST(a.File_UsedMB AS bigint), '#,##0')          AS Used
      , CONCAT(FORMAT(a.File_UsedPerc, '##0.00'), '%')          AS [Used%]
      , FORMAT(CAST(a.File_FreeMB AS decimal(12, 0)), '#,##0')  AS Free
      , a.GroupName                                             AS FileGroup
      --a.FileType ,
      --a.FileName ,
      , a.ShrinkMeBaby1MoreTime                                 AS ShrinkMe
      , a.ExpandMeInAdvance                                     AS ExpandMe
      , a.FileFullPath                                          AS FullPath
    FROM
    (
        SELECT CAST(1 AS smallint)                                                                                                                                                       AS DetailType
             , CONCAT('        --> ', mf.name)                                                                                                                                           AS [Disk/DB/File]
             , UPPER(LEFT(mf.volume_mount_point, 1))                                                                                                                                     AS DriveLetter
             --mf.logical_volume_name AS DriveLogicalName ,
             , fl.DBName
             --CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0) AS Disk_TotalSpace_MB,
             --CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0) AS Disk_UsedSpace_MB,
             --CONVERT(DECIMAL(5,2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.) Disk_UsedSpace_Perc,
             --CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0) AS Disk_FreeSpace_MB,
             , ISNULL(fl.File_TotalMB, 0)                                                                                                                                                AS File_TotalMB
             , ISNULL(fl.File_UsedMB, 0)                                                                                                                                                 AS File_UsedMB
             --CONVERT(DECIMAL(5,2), ISNULL(fl.File_UsedMB,0)/ISNULL(fl.File_TotalMB,0) * 100.) AS File_UsedPerc,
             , CONVERT(decimal(5, 2), ISNULL(fl.File_UsedMB, 0) / ISNULL(IIF(fl.File_TotalMB = 0, 1, fl.File_TotalMB), 1) * 100.)                                                        AS File_UsedPerc
             , ISNULL(fl.File_FreeMB, 0)                                                                                                                                                 AS File_FreeMB
             , fl.GroupName
             , mf.type_desc                                                                                                                                                              AS FileType
             , mf.name                                                                                                                                                                   AS FileName
             , CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(decimal(12, 0), ISNULL(fl.File_UsedMB, 0)) + 2000, ')   -- USE [', DB_NAME(mf.database_id), ']')                        AS ShrinkMeBaby1MoreTime
             , CONCAT('ALTER DATABASE [', DB_NAME(mf.database_id), '] MODIFY FILE (NAME = ''', mf.name, ''', SIZE = ', CEILING(fl.File_TotalMB * 1.5 / 1024), 'GB, FILEGROWTH = 1GB );') AS ExpandMeInAdvance
             , mf.physical_name                                                                                                                                                          AS FileFullPath
        FROM #masterFilesList    AS mf
            LEFT JOIN #filesList AS fl
                ON fl.DId = mf.database_id
                   AND fl.fileid = mf.file_id
        WHERE ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
              AND
              (
                  ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0
                  OR fl.DBName LIKE CONCAT('%', @Databases, '%')
              )
        --ORDER BY DriveLetter ASC, fl.DBName ASC
        UNION ALL
        SELECT CAST(2 AS smallint)                                                                                                                 AS DetailType
             , CONCAT(IIF(@ReportType = 3, '', '   --> '), fl.DBName)                                                                              AS [Disk/DB/File]
             , IIF(@ReportType = 2, UPPER(LEFT(mf.volume_mount_point, 1)), '')                                                                     AS DriveLetter
                                                                                                                                                                            --mf.logical_volume_name AS DriveLogicalName ,
             , fl.DBName
                                                                                                                                                                            --mf.name [FileName], 
                                                                                                                                                                            --mf.type_desc FileType,
                                                                                                                                                                            --fl.GroupName,
                                                                                                                                                                            --MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) AS Disk_TotalSpace_MB,
                                                                                                                                                                            --MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) AS Disk_UsedSpace_MB,
                                                                                                                                                                            --CONVERT(DECIMAL(5,2), ((SUM(mf.total_bytes) - SUM(mf.available_bytes) - 1.) / SUM(mf.total_bytes)) * 100.) Disk_UsedSpace_Perc,
                                                                                                                                                                            --MAX(CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0)) AS Disk_FreeSpace_MB,
             , SUM(ISNULL(fl.File_TotalMB, 0))                                                                                                     AS File_TotalMB
             , SUM(ISNULL(fl.File_UsedMB, 0))                                                                                                      AS File_UsedMB
             , CAST((SUM(ISNULL(fl.File_UsedMB, 0) * 1.) / SUM(ISNULL(IIF(fl.File_TotalMB = 0, 1, fl.File_TotalMB), 1))) * 100. AS decimal(12, 2)) AS Perc
                                                                                                                                                                            --CONVERT(DECIMAL(12,2), ((MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_FreeMB,0))) / (MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) )) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc,
             , SUM(ISNULL(fl.File_FreeMB, 0))                                                                                                      AS File_FreeMB
             , ''                                                                                                                                  AS GroupName             --fl.GroupName,
             , ''                                                                                                                                  AS FileType              --mf.type_desc FileType,
             , ''                                                                                                                                  AS FileName              --mf.name [FileName], 
             , ''                                                                                                                                  AS ShrinkMeBaby1MoreTime --CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(DECIMAL(12, 0), ISNULL(fl.File_UsedMB,0)) + 2000, ')   -- USE [',DB_NAME(mf.database_id),']') ShrinkMeBaby1MoreTime,
             , ''                                                                                                                                  AS ExpandMeInAdvance     --CONCAT('USE master; ALTER DATABASE [', DB_NAME(mf.database_id), '] MODIFY FILE (NAME = ''', mf.name, ''', SIZE = ', CAST(fl.File_UsedMB * 1.5 AS int), 'MB, FILEGROWTH = 1GB );')
             , ''                                                                                                                                  AS FileFullPath          --mf.physical_name FileFullPath
        --mf.physical_name FileFullPath
        FROM #masterFilesList    AS mf
            LEFT JOIN #filesList AS fl
                ON fl.DId = mf.database_id
                   AND fl.fileid = mf.file_id
        WHERE ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
              AND
              (
                  ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0
                  OR fl.DBName LIKE CONCAT('%', @Databases, '%')
              )
        GROUP BY fl.DBName
               , IIF(@ReportType = 2, UPPER(LEFT(mf.volume_mount_point, 1)), '')
        --mf.logical_volume_name 
        --ORDER BY DriveLetter ASC, DBName ASC
        UNION ALL
        SELECT CAST(3 AS smallint)                                                                                                                                                          AS DetailType
             , CONCAT(IIF(@ReportType = 3, '   --> ', ''), UPPER(LEFT(mf.volume_mount_point, 1)), ': ', IIF(LEN(mf.logical_volume_name) > 0, CONCAT('(', mf.logical_volume_name, ')'), '')) AS [Disk/DB/File]
             , UPPER(LEFT(mf.volume_mount_point, 1))                                                                                                                                        AS DriveLetter
                                                                                                                                                                                                                     --mf.logical_volume_name AS DriveLogicalName ,
             , IIF(@ReportType = 3, fl.DBName, '')                                                                                                                                          AS DBName
                                                                                                                                                                                                                     --MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) AS Disk_TotalSpace_MB,
                                                                                                                                                                                                                     --MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) AS Disk_UsedSpace_MB,
                                                                                                                                                                                                                     --MAX(CONVERT(DECIMAL(5,2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.)) Disk_UsedSpace_Perc,
                                                                                                                                                                                                                     --MAX(CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0)) AS Disk_FreeSpace_MB,
             , IIF(@ReportType = 2, MAX(CONVERT(decimal(12, 2), mf.total_bytes / 1048576.0)), SUM(ISNULL(fl.File_TotalMB, 0)))
             , IIF(@ReportType = 2, MAX(CONVERT(decimal(12, 2), (mf.total_bytes - mf.available_bytes) / 1048576.0)), SUM(ISNULL(fl.File_UsedMB, 0)))
             , IIF(@ReportType = 2, MAX(CONVERT(decimal(5, 2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.)), CAST(SUM((ISNULL(fl.File_UsedMB, 0) * 1.)) / SUM(ISNULL(IIF(fl.File_TotalMB = 0, 1, fl.File_TotalMB), 0)) * 100. AS decimal(12, 2)))
             , IIF(@ReportType = 2, MAX(CONVERT(decimal(12, 2), mf.available_bytes / 1048576.0)), SUM(ISNULL(fl.File_FreeMB, 0)))
                                                                                                                                                                                                                     --SUM(ISNULL(fl.File_TotalMB,0)) DBFiles_TotalMB,
                                                                                                                                                                                                                     --SUM(ISNULL(fl.File_UsedMB,0)) DBFiles_UsedMB,
                                                                                                                                                                                                                     --CAST(SUM((ISNULL(fl.File_UsedMB,0) * 1.)) / SUM(ISNULL(fl.File_TotalMB,0)) * 100. AS DECIMAL(12,2)) AS Perc,
                                                                                                                                                                                                                     ----CONVERT(DECIMAL(12,2), ((MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_FreeMB,0))) / (MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) )) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc,
                                                                                                                                                                                                                     --SUM(ISNULL(fl.File_FreeMB,0)) DBFiles_FreeMB,
             , ''                                                                                                                                                                           AS GroupName             --fl.GroupName,
             , ''                                                                                                                                                                           AS FileType              --mf.type_desc FileType,
             , ''                                                                                                                                                                           AS FileName              --mf.name [FileName], 
             , ''                                                                                                                                                                           AS ShrinkMeBaby1MoreTime --CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(DECIMAL(12, 0), ISNULL(fl.File_UsedMB,0)) + 2000, ')   -- USE [',DB_NAME(mf.database_id),']') ShrinkMeBaby1MoreTime,
             , ''                                                                                                                                                                           AS ExpandMeInAdvance     --CONCAT('USE master; ALTER DATABASE [', DB_NAME(mf.database_id), '] MODIFY FILE (NAME = ''', mf.name, ''', SIZE = ', CAST(fl.File_UsedMB * 1.5 AS int), 'MB, FILEGROWTH = 1GB );')
             , ''                                                                                                                                                                           AS FileFullPath          --mf.physical_name FileFullPath

        --MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_TotalMB,0.)) AS Disk_UsedSpace_NonDB_MB,
        --CONVERT(DECIMAL(12,2), ((MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_FreeMB,0))) / (MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) )) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc

        FROM #masterFilesList    AS mf
            LEFT JOIN #filesList AS fl
                ON fl.DId = mf.database_id
                   AND fl.fileid = mf.file_id
        WHERE ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
              AND
              (
                  ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0
                  OR fl.DBName LIKE CONCAT('%', @Databases, '%')
              )
        GROUP BY UPPER(LEFT(mf.volume_mount_point, 1))
               , mf.logical_volume_name
               , IIF(@ReportType = 3, fl.DBName, '')
    --ORDER BY DriveLetter
    ) AS a
    --WHERE a.DetailType <> 1
    ORDER BY IIF(@ReportType = 3, a.DBName, a.DriveLetter) ASC
           , IIF(@ReportType = 3, a.DriveLetter, a.DBName) ASC
           , a.FileName;
END;


SELECT UPPER(LEFT(mf.volume_mount_point, 1))                                                                                                                                                                                                            AS DriveLetter
     , mf.logical_volume_name                                                                                                                                                                                                                           AS DriveLogicalName
     , MAX(CONVERT(decimal(12, 2), mf.total_bytes / 1024.0 / 1024 / 1024))                                                                                                                                                                              AS Disk_TotalSpace_GB
     , MAX(CONVERT(decimal(12, 2), (mf.total_bytes - mf.available_bytes) / 1024.0 / 1024 / 1024))                                                                                                                                                       AS Disk_UsedSpace_GB
     , MAX(CONVERT(decimal(12, 2), mf.available_bytes / 1024.0 / 1024 / 1024))                                                                                                                                                                          AS Disk_FreeSpace_GB
     , MAX(CONVERT(decimal(12, 2), (mf.total_bytes - mf.available_bytes) / 1024.0 / 1024 / 1024)) - SUM(ISNULL(fl.File_TotalMB / 1024.0, 0.))                                                                                                           AS Disk_UsedSpace_NonDB_GB
     , SUM(ISNULL(fl.File_TotalMB / 1024.0, 0))                                                                                                                                                                                                         AS DBFiles_Total_GB
     , SUM(ISNULL(fl.File_UsedMB / 1024.0, 0))                                                                                                                                                                                                          AS DBFiles_Used_GB
     , SUM(ISNULL(fl.File_FreeMB / 1024.0, 0))                                                                                                                                                                                                          AS DBFiles_Free_GB
     , MAX(CONVERT(decimal(5, 2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.))                                                                                                                                                AS Disk_UsedSpace_Perc
     , CONVERT(decimal(12, 2), ((MAX(CONVERT(decimal(12, 2), (mf.total_bytes - mf.available_bytes) / 1024.0 / 1024 / 1024)) - SUM(ISNULL(fl.File_FreeMB / 1024.0, 0))) / (MAX(CONVERT(decimal(12, 2), mf.total_bytes / 1024.0 / 1024 / 1024)))) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc
FROM #masterFilesList    AS mf
    LEFT JOIN #filesList AS fl
        ON fl.DId = mf.database_id
           AND fl.fileid = mf.file_id
WHERE ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
      AND
      (
          ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0
          OR fl.DBName LIKE CONCAT('%', @Databases, '%')
      )
GROUP BY UPPER(LEFT(mf.volume_mount_point, 1))
       , mf.logical_volume_name
ORDER BY DriveLetter;

