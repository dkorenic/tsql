/*    ==Scripting Parameters==

    Source Server Version : SQL Server 2016 (13.0.4411)
    Source Database Engine Edition : Microsoft SQL Server Enterprise Edition
    Source Database Engine Type : Standalone SQL Server

    Target Server Version : SQL Server 2017
    Target Database Engine Edition : Microsoft SQL Server Standard Edition
    Target Database Engine Type : Standalone SQL Server
*/
USE [master]
GO
/****** Object:  StoredProcedure [dbo].[sp_spaceused8]    Script Date: 11/2/2017 7:22:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_spaceused8]
@DriveLetters NVARCHAR(100) = NULL,
@Databases NVARCHAR(100) = NULL,
@ReportType TINYINT = 2
AS
/*
2017-07-18 created by TuljanWorks - Jira task: no jira, no task ...

EXEC dbo.sp_spaceused8 @DriveLetters = 'CDE', @Databases = 'master'
EXEC dbo.sp_spaceused8 @Databases = 'clientblue'
EXEC dbo.sp_spaceused8 @reporttype=2 , @Databases = NULL
*/


DECLARE @id INT = -1, @sql NVARCHAR(MAX) = '', @db sysname

IF LEN(@Databases) = 0 SET @Databases = NULL
IF LEN(@DriveLetters) = 0 SET @DriveLetters = NULL
IF @ReportType IS NULL OR @ReportType NOT BETWEEN 1 AND 3 SET @ReportType = 1

IF OBJECT_ID('tempdb..#filesList') IS NOT NULL DROP TABLE #filesList
CREATE TABLE #filesList (
	DId INT,
	DBName NVARCHAR(100),
	fileid SMALLINT,
	GroupName sysname,
	File_TotalMB DECIMAL(12, 2),
	File_UsedMB DECIMAL(12, 2),
	File_FreeMB DECIMAL(12, 2)
)

SELECT	name,
		database_id
INTO	#db
FROM	sys.databases WITH (NOLOCK);

WHILE 1 = 1
BEGIN
	SELECT	TOP 1 
			@db = name
		,	@id = database_id
	FROM	#db WITH (NOLOCK)
	WHERE	database_id > @id
	ORDER BY database_id

	IF @@ROWCOUNT = 0 BREAK

	SET @sql = CONCAT(CAST(N'' AS NVARCHAR(MAX)),
'
USE [',@db,']
SELECT  ''',@id,''' As DId,
		''',@db,''' As DBName,
		[fileid] ,
		ISNULL(fg.groupname, ''(log)'') AS GroupName,
        CONVERT(DECIMAL(12, 2), ROUND([size] / 128.000, 2)) AS [File_TotalMB] ,
        CONVERT(DECIMAL(12, 2), ROUND(FILEPROPERTY([name], ''SpaceUsed'') / 128.000, 2)) AS [File_UsedMB] ,
        CONVERT(DECIMAL(12, 2), ROUND(( [size] - FILEPROPERTY([name], ''SpaceUsed'') ) / 128.000, 2)) AS [File_FreeMB]
FROM    [sys].[sysfiles] f WITH (NOLOCK)
LEFT JOIN	sysfilegroups fg WITH (NOLOCK) ON f.groupid = fg.groupid
'
	)

	INSERT INTO #filesList 
	EXECUTE (@sql)

END

SELECT	mf.database_id, mf.file_id, mf.name, mf.type_desc, dovs.volume_mount_point, dovs.logical_volume_name, dovs.total_bytes, dovs.available_bytes, mf.physical_name
INTO	#masterFilesList
FROM    sys.master_files mf WITH (NOLOCK)
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) dovs  --WITH (NOLOCK)

IF @ReportType = 1
BEGIN
	SELECT	UPPER(LEFT(mf.volume_mount_point, 1)) AS DriveLetter ,
			--mf.logical_volume_name AS DriveLogicalName ,
			fl.DBName ,
			mf.name [FileName], 
			--mf.type_desc FileType,
			fl.GroupName,
			CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0) AS Disk_TotalSpace_MB,
			CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0) AS Disk_UsedSpace_MB,
			CONVERT(DECIMAL(5,2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.) Disk_UsedSpace_Perc,
			CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0) AS Disk_FreeSpace_MB,
			ISNULL(fl.File_TotalMB,0) File_TotalMB,
			ISNULL(fl.File_UsedMB,0) File_UsedMB,
			CONVERT(DECIMAL(5,2), ISNULL(fl.File_UsedMB,0)/ISNULL(IIF(fl.File_TotalMB=0,1,fl.File_TotalMB),1) * 100.) AS File_UsedPerc,
			ISNULL(fl.File_FreeMB,0) File_FreeMB,
			CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(DECIMAL(12, 0), ISNULL(fl.File_UsedMB,0)) + 2000, ')   -- USE [',DB_NAME(mf.database_id),']') ShrinkMeBaby1MoreTime,
			mf.physical_name FileFullPath
	FROM    #masterFilesList mf
	LEFT JOIN #filesList fl ON fl.DId = mf.database_id AND fl.fileid = mf.file_id
	WHERE	ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
		AND (ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0 OR fl.DBName LIKE CONCAT('%', @Databases, '%'))
	ORDER BY DriveLetter ASC, fl.DBName ASC

	SELECT	UPPER(LEFT(mf.volume_mount_point, 1)) AS DriveLetter ,
			--mf.logical_volume_name AS DriveLogicalName ,
			fl.DBName ,
			--mf.name [FileName], 
			--mf.type_desc FileType,
			--fl.GroupName,
			MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) AS Disk_TotalSpace_MB,
			MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) AS Disk_UsedSpace_MB,
			CONVERT(DECIMAL(5,2), (((SUM(mf.total_bytes) - SUM(mf.available_bytes)) - 1.) / SUM(mf.total_bytes)) * 100.) Disk_UsedSpace_Perc,
			MAX(CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0)) AS Disk_FreeSpace_MB,
			SUM(ISNULL(fl.File_TotalMB,0)) File_TotalMB,
			SUM(ISNULL(fl.File_UsedMB,0)) File_UsedMB,
			SUM(ISNULL(fl.File_FreeMB,0)) File_FreeMB
			--mf.physical_name FileFullPath
	FROM    #masterFilesList mf
	LEFT JOIN #filesList fl ON fl.DId = mf.database_id AND fl.fileid = mf.file_id
	WHERE	ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
		AND (ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0 OR fl.DBName LIKE CONCAT('%', @Databases, '%'))
	GROUP BY fl.DBName ,
			UPPER(LEFT(mf.volume_mount_point, 1)) ,
			mf.logical_volume_name 
	ORDER BY DriveLetter ASC, DBName ASC


	SELECT	UPPER(LEFT(mf.volume_mount_point, 1)) AS DriveLetter ,
			mf.logical_volume_name AS DriveLogicalName ,
       
			MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) AS Disk_TotalSpace_MB,
			MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) AS Disk_UsedSpace_MB,
			MAX(CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0)) AS Disk_FreeSpace_MB,
			MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_TotalMB,0.)) AS Disk_UsedSpace_NonDB_MB,
		
			SUM(ISNULL(fl.File_TotalMB,0)) DBFiles_TotalMB,
			SUM(ISNULL(fl.File_UsedMB,0)) DBFiles_UsedMB,
			SUM(ISNULL(fl.File_FreeMB,0)) DBFiles_FreeMB,

			MAX(CONVERT(DECIMAL(5,2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.)) Disk_UsedSpace_Perc,
			CONVERT(DECIMAL(12,2), ((MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_FreeMB,0))) / (MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) )) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc
	FROM    #masterFilesList mf
	LEFT JOIN #filesList fl ON fl.DId = mf.database_id AND fl.fileid = mf.file_id
	WHERE	ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
		AND (ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0 OR fl.DBName LIKE CONCAT('%', @Databases, '%'))
	GROUP BY UPPER(LEFT(mf.volume_mount_point, 1)) ,
			 mf.logical_volume_name
	ORDER BY DriveLetter
END
ELSE

BEGIN

	SELECT --a.DetailType ,
           a.[Disk/DB/File] ,
           --a.DriveLetter ,
           --a.DBName ,
           --a.Disk_TotalSpace_MB ,
           --a.Disk_UsedSpace_MB ,
           --a.Disk_UsedSpace_Perc ,
           --a.Disk_FreeSpace_MB ,
           FORMAT(CAST(a.File_TotalMB AS DECIMAL(12,0)), '#,##0') AS TotalMB,
           FORMAT(CAST(a.File_UsedMB AS BIGINT), '#,##0') Used,
           CONCAT(FORMAT(a.File_UsedPerc, '##0.00'), '%') [Used%],
           FORMAT(CAST(a.File_FreeMB AS DECIMAL(12,0)), '#,##0') Free,
           a.GroupName AS [FileGroup],
           --a.FileType ,
           --a.FileName ,
           a.ShrinkMeBaby1MoreTime AS ShrinkMe,
           a.FileFullPath AS FullPath
	FROM (
		SELECT	CAST(1 AS SMALLINT) AS DetailType,
				CONCAT('        --> ', mf.name) [Disk/DB/File], 
			
				UPPER(LEFT(mf.volume_mount_point, 1)) AS DriveLetter ,
				--mf.logical_volume_name AS DriveLogicalName ,
				fl.DBName ,

				--CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0) AS Disk_TotalSpace_MB,
				--CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0) AS Disk_UsedSpace_MB,
				--CONVERT(DECIMAL(5,2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.) Disk_UsedSpace_Perc,
				--CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0) AS Disk_FreeSpace_MB,
				ISNULL(fl.File_TotalMB,0) File_TotalMB,
				ISNULL(fl.File_UsedMB,0) File_UsedMB,
				--CONVERT(DECIMAL(5,2), ISNULL(fl.File_UsedMB,0)/ISNULL(fl.File_TotalMB,0) * 100.) AS File_UsedPerc,
				CONVERT(DECIMAL(5,2), ISNULL(fl.File_UsedMB,0)/ISNULL(IIF(fl.File_TotalMB=0,1,fl.File_TotalMB),1) * 100.) AS File_UsedPerc,
				ISNULL(fl.File_FreeMB,0) File_FreeMB,
				fl.GroupName,
				mf.type_desc FileType,
				mf.name [FileName], 
				CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(DECIMAL(12, 0), ISNULL(fl.File_UsedMB,0)) + 2000, ')   -- USE [',DB_NAME(mf.database_id),']') ShrinkMeBaby1MoreTime,
				mf.physical_name FileFullPath
		FROM    #masterFilesList mf
		LEFT JOIN #filesList fl ON fl.DId = mf.database_id AND fl.fileid = mf.file_id
		WHERE	ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
			AND (ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0 OR fl.DBName LIKE CONCAT('%', @Databases, '%'))
		--ORDER BY DriveLetter ASC, fl.DBName ASC
		UNION ALL
		SELECT	CAST(2 AS SMALLINT) AS DetailType,
				CONCAT(IIF(@ReportType = 3, '', '   --> '), fl.DBName) AS [Disk/DB/File],
				IIF(@ReportType = 2, UPPER(LEFT(mf.volume_mount_point, 1)), '') AS DriveLetter ,
				--mf.logical_volume_name AS DriveLogicalName ,
				fl.DBName ,
				--mf.name [FileName], 
				--mf.type_desc FileType,
				--fl.GroupName,
				--MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) AS Disk_TotalSpace_MB,
				--MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) AS Disk_UsedSpace_MB,
				--CONVERT(DECIMAL(5,2), ((SUM(mf.total_bytes) - SUM(mf.available_bytes) - 1.) / SUM(mf.total_bytes)) * 100.) Disk_UsedSpace_Perc,
				--MAX(CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0)) AS Disk_FreeSpace_MB,
				SUM(ISNULL(fl.File_TotalMB,0)) File_TotalMB,
				SUM(ISNULL(fl.File_UsedMB,0)) File_UsedMB,
				CAST((SUM(ISNULL(fl.File_UsedMB,0) * 1.) / SUM(ISNULL(IIF(fl.File_TotalMB=0,1,fl.File_TotalMB),1))) * 100. AS DECIMAL(12,2)) Perc,
				--CONVERT(DECIMAL(12,2), ((MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_FreeMB,0))) / (MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) )) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc,
				SUM(ISNULL(fl.File_FreeMB,0)) File_FreeMB,
				'' AS GroupName, --fl.GroupName,
				'' AS FileType, --mf.type_desc FileType,
				'' AS [FileName], --mf.name [FileName], 
				'' AS ShrinkMeBaby1MoreTime, --CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(DECIMAL(12, 0), ISNULL(fl.File_UsedMB,0)) + 2000, ')   -- USE [',DB_NAME(mf.database_id),']') ShrinkMeBaby1MoreTime,
				'' AS FileFullPath --mf.physical_name FileFullPath
				--mf.physical_name FileFullPath
		FROM    #masterFilesList mf
		LEFT JOIN #filesList fl ON fl.DId = mf.database_id AND fl.fileid = mf.file_id
		WHERE	ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
			AND (ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0 OR fl.DBName LIKE CONCAT('%', @Databases, '%'))
		GROUP BY fl.DBName ,
				IIF(@ReportType = 2, UPPER(LEFT(mf.volume_mount_point, 1)), '') 
				--mf.logical_volume_name 
		--ORDER BY DriveLetter ASC, DBName ASC
		UNION ALL
		SELECT	CAST(3 AS SMALLINT) AS DetailType,
				
				CONCAT(IIF(@ReportType = 3, '   --> ', ''), UPPER(LEFT(mf.volume_mount_point, 1)), ': ', IIF(LEN(mf.logical_volume_name) > 0, CONCAT('(', mf.logical_volume_name, ')'), '' )) [Disk/DB/File],
				UPPER(LEFT(mf.volume_mount_point, 1)) AS DriveLetter ,
				--mf.logical_volume_name AS DriveLogicalName ,
				IIF(@ReportType = 3, fl.DBName, '') AS DBName,
       
				--MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) AS Disk_TotalSpace_MB,
				--MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) AS Disk_UsedSpace_MB,
				--MAX(CONVERT(DECIMAL(5,2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.)) Disk_UsedSpace_Perc,
				--MAX(CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0)) AS Disk_FreeSpace_MB,

				IIF(@ReportType = 2,MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)),SUM(ISNULL(fl.File_TotalMB,0))),
				IIF(@ReportType = 2,MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)),SUM(ISNULL(fl.File_UsedMB,0))),
				IIF(@ReportType = 2,MAX(CONVERT(DECIMAL(5,2), ((mf.total_bytes - mf.available_bytes - 1.) / mf.total_bytes) * 100.)),CAST(SUM((ISNULL(fl.File_UsedMB,0) * 1.)) / SUM(ISNULL(IIF(fl.File_TotalMB=0,1,fl.File_TotalMB),0)) * 100. AS DECIMAL(12,2))),
				IIF(@ReportType = 2,MAX(CONVERT(DECIMAL(12,2), mf.available_bytes / 1048576.0)),SUM(ISNULL(fl.File_FreeMB,0))),
		
				--SUM(ISNULL(fl.File_TotalMB,0)) DBFiles_TotalMB,
				--SUM(ISNULL(fl.File_UsedMB,0)) DBFiles_UsedMB,
				--CAST(SUM((ISNULL(fl.File_UsedMB,0) * 1.)) / SUM(ISNULL(fl.File_TotalMB,0)) * 100. AS DECIMAL(12,2)) AS Perc,
				----CONVERT(DECIMAL(12,2), ((MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_FreeMB,0))) / (MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) )) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc,
				--SUM(ISNULL(fl.File_FreeMB,0)) DBFiles_FreeMB,

				'' AS GroupName, --fl.GroupName,
				'' AS FileType, --mf.type_desc FileType,
				'' AS [FileName], --mf.name [FileName], 
				'' AS ShrinkMeBaby1MoreTime, --CONCAT('dbcc shrinkfile(', mf.name, ', ', CONVERT(DECIMAL(12, 0), ISNULL(fl.File_UsedMB,0)) + 2000, ')   -- USE [',DB_NAME(mf.database_id),']') ShrinkMeBaby1MoreTime,
				'' AS FileFullPath --mf.physical_name FileFullPath

				--MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_TotalMB,0.)) AS Disk_UsedSpace_NonDB_MB,
				--CONVERT(DECIMAL(12,2), ((MAX(CONVERT(DECIMAL(12,2), (mf.total_bytes - mf.available_bytes) / 1048576.0)) - SUM(ISNULL(fl.File_FreeMB,0))) / (MAX(CONVERT(DECIMAL(12,2), mf.total_bytes / 1048576.0)) )) * 100.) AS Disk_UsedSpaceFromDBFiles_Perc
			
		FROM    #masterFilesList mf
		LEFT JOIN #filesList fl ON fl.DId = mf.database_id AND fl.fileid = mf.file_id
		WHERE	ISNULL(CHARINDEX(LEFT(mf.volume_mount_point, 1), @DriveLetters), 1) > 0
			AND (ISNULL(CHARINDEX(fl.DBName, @Databases), 1) > 0 OR fl.DBName LIKE CONCAT('%', @Databases, '%'))
		GROUP BY UPPER(LEFT(mf.volume_mount_point, 1)) ,
				 mf.logical_volume_name,
				 IIF(@ReportType = 3, fl.DBName, '')
		--ORDER BY DriveLetter
	) a
	--WHERE a.DetailType <> 1
	ORDER BY	IIF(@ReportType = 3, a.DBName, a.DriveLetter) ASC
			,	IIF(@ReportType = 3, a.DriveLetter, a.DBName) ASC
			,	a.FileName
END




GO
