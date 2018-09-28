USE CasaTerm;

--DBCC SHRINKFILE (N'ABCbet' , 110905)
DECLARE @name sysname
      , @size int, @free int
	  , @sql nvarchar(MAX) ;

WHILE 1 = 1
BEGIN
    WITH s AS (
        SELECT (f.file_id)                                                                                            AS File_Id
             , f.type_desc
             , ROUND(CAST((f.size) AS float) / 128, 2)                                                                AS Reserved_MB
             , ROUND(CAST((FILEPROPERTY(f.name, 'SpaceUsed')) AS float) / 128, 2)                                     AS Used_MB
             , ROUND((CAST((f.size) AS float) / 128) - (CAST((FILEPROPERTY(f.name, 'SpaceUsed')) AS float) / 128), 2) AS Free_MB
             , f.name
             , f.physical_name
        FROM sys.database_files      AS f
            LEFT JOIN sys.filegroups AS fg
                ON f.data_space_id = fg.data_space_id
    )
    SELECT TOP 1
        @name = s.name
      , @size = s.Reserved_MB
	  , @free = s.Free_MB
    FROM s
    WHERE 1 = 1
          AND s.type_desc = 'ROWS'
          AND s.Free_MB > 10240;
    IF @@ROWCOUNT = 0
        BREAK;

	PRINT CONCAT('db: ', DB_NAME(), ', file: ', @name, ', size: ', @size, ', free: ', @free)

	SET @sql = CONCAT('DBCC SHRINKFILE (N''', @name, ''' , ', @size - 1024, ')')

	PRINT @sql

	EXEC (@sql)

	BREAK
END;
