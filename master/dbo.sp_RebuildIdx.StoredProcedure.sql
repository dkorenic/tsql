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
/****** Object:  StoredProcedure [dbo].[sp_RebuildIdx]    Script Date: 11/2/2017 7:22:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_RebuildIdx]
    @database NVARCHAR(128) = NULL,
    @schema NVARCHAR(128) = NULL,
    @table NVARCHAR(128) = NULL,
    @index NVARCHAR(128) = NULL,
    @partitionFrom INT = NULL,
    @partitionTo INT = NULL,
    @fragmentation INT = 15,
    @pages INT = 500,
    @sortInTempDb BIT = 0,
    @fillFactor INT = NULL,
    @maxdop TINYINT = NULL,
    @reorganizeLimit TINYINT = 40,
    @rebuildHeap BIT = 1,
    @rebuildClustered BIT = 1,
    @rebuildNonclustered BIT = 1,
    @rebuildXML BIT = 1,
    @rebuildSpatial BIT = 1,
    @rebuildClColumnstore BIT = 1,
    @rebuildNcColumnstore BIT = 1,
    @rebuildOnline VARCHAR(4) = 'AUTO',  -- 'YES', 'NO', 'AUTO' 
    @scanMode VARCHAR(10) = 'LIMITED',   -- LIMITED, SAMPLED, or DETAILED
    @runInParallel VARCHAR(6) = 'NO',    --'ALL' - tries to rebuild all indexes in parallel, 'TABLES' groups indexes belonging to tables in batches and executes batches in parallel, 'NO' - no parallelism
    @waitAtLowPriorityDuration INT = NULL,
    @waitAtLowPriorityAbort VARCHAR(10) = NULL,
    @waitBetweenRebuilds CHAR(8) = '00:00:00',
    @scanOnly BIT = 1,
    @forceCompression VARCHAR(4) = NULL, --if not null forces compression on partitions
    @dryRun BIT = 0                      -- Only checks index fragmentation and generates commands that would be executed. Prints results.
AS -- =============================================
    -- Author:		Andrej Mihajlovic
    -- Create date: 2014-07-21
    -- Description:	
    -- =============================================

    SET @database = REPLACE(REPLACE(@database, '[', ''), ']', '');
    SET @schema = REPLACE(REPLACE(@schema, '[', ''), ']', '');
    SET @table = REPLACE(REPLACE(@table, '[', ''), ']', '');

    IF (@scanOnly <> 1)
    BEGIN
        IF (UPPER(@runInParallel) NOT IN ( 'NO', 'ALL', 'TABLES' ))
        BEGIN
            RAISERROR(
                         'ERROR: Wrong value specified for @runInParallel! Accepted values: ''NO'', ''ALL'' OR ''TABLES''.',
                         16,
                         16
                     );
            RETURN;
        END;


        IF (UPPER(@rebuildOnline) NOT IN ( 'NO', 'YES', 'AUTO' ))
        BEGIN
            RAISERROR(
                         'ERROR: Wrong value specified for @rebuildOnline! Accepted values: ''NO'', ''YES'' OR ''AUTO''.',
                         16,
                         16
                     );
            RETURN;
        END;

        DECLARE @isEnterprise BIT = CASE
                                        WHEN SERVERPROPERTY('EngineEdition') = 3 THEN
                                            1
                                        ELSE
                                            0
                                    END;
        DECLARE @online BIT = CASE
                                  WHEN UPPER(@rebuildOnline) = 'AUTO' THEN
                                      @isEnterprise
                                  WHEN UPPER(@rebuildOnline) = 'NO' THEN
                                      0
                                  WHEN UPPER(@rebuildOnline) = 'YES' THEN
                                      1
                              END;

        IF (@online = 1 AND @isEnterprise <> 1)
        BEGIN
            RAISERROR('ERROR: Online index rebuild is available only in enterprise and developer editions.', 16, 16);
            RETURN;
        END;

        IF (
               UPPER(@runInParallel) <> 'NO'
               AND
            (
                NOT EXISTS
        (
            SELECT name
            FROM [sys].[procedures]
            WHERE name = 'EnqueueTask'
                  AND OBJECT_SCHEMA_NAME([object_id]) = 'pp'
        )
                OR NOT EXISTS
        (
            SELECT name
            FROM [sys].[procedures]
            WHERE name = 'WaitForTaskGroup'
                  AND OBJECT_SCHEMA_NAME([object_id]) = 'pp'
        )
                OR NOT EXISTS
        (
            SELECT name
            FROM [sys].[procedures]
            WHERE name = 'GetTaskGroupId'
                  AND OBJECT_SCHEMA_NAME([object_id]) = 'pp'
        )
            )
           )
        BEGIN
            RAISERROR('ERROR: Parallel pack not installed! Set @runInParallel = ''NO''.', 16, 16);
            RETURN;
        END;
        ELSE
        BEGIN
            IF (@isEnterprise = 0 AND UPPER(@runInParallel) = 'ALL')
            BEGIN
                RAISERROR(
                             'WARN: Only enterprise and developer editions benefit from rebuilding indexes on a single table in parallel.',
                             1,
                             1
                         );
            END;
        END;
    END;

    SET NOCOUNT ON;

    DECLARE @indexTable TABLE
    (
        indexId INT,
        indexName NVARCHAR(130),
        fragmentation FLOAT,
        tableName NVARCHAR(261),
        isPartitioned BIT,
        partitionNumber INT,
        dataCompression VARCHAR(20),
        typeId TINYINT,
        fillFact TINYINT
    );

    DECLARE @objectId INT = NULL,
        @index_Id INT = NULL;


    SET @database = ISNULL(@database, DB_NAME());
    IF (@schema IS NOT NULL AND @table IS NOT NULL)
    BEGIN
        SET @objectId = OBJECT_ID(CONCAT(QUOTENAME(@database),'.', QUOTENAME(@schema) , '.' + QUOTENAME(@table)));
    END;

    IF (@index IS NOT NULL)
    BEGIN
        DECLARE @indexIdSql NVARCHAR(4000)
            = N'
				USE [' + @database
              + N']
				SELECT @indexId = index_id FROM sys.indexes where name = @indexName and object_id = @objectId';
        EXEC [sys].[sp_executesql] @indexIdSql,
            N'@indexId int OUTPUT, @indexName nvarchar(128), @objectId int',
            @indexName = @index,
            @indexId = @index_Id OUTPUT,
            @objectId = @objectId;
    END;

    SET @partitionFrom = ISNULL(@partitionFrom, 0);
    SET @partitionTo = ISNULL(@partitionTo, 200000);

    DECLARE @indexSqlParams NVARCHAR(MAX)
        = '
	@database				NVARCHAR(128),
	@schema					NVARCHAR(128),
	@table					NVARCHAR(128),
	@partitionFrom			INT,
    @partitionTo			INT,
	@fragmentation			INT,
	@pages					INT,
	@fillFactor				INT,
	@rebuildHeap			BIT,
	@rebuildClustered		BIT,
	@rebuildNonclustered	BIT,
	@rebuildXML				BIT,
	@rebuildSpatial			BIT,
	@rebuildClColumnstore	BIT,
	@rebuildNcColumnstore	BIT,
	@scanMode				VARCHAR(10),
	@objectId				INT,
	@indexId				INT,
	@forceCompression VARCHAR(4)
	';
    DECLARE @indexSql NVARCHAR(MAX)
        = CONCAT(
                    N'
		USE ['                                                     + @database + N']
		SELECT 	
					--[',
                    @schema,
                    '].[',
                    @table,
                    ']

				i.index_id
				, ''['' + i.[name] + '']'' AS [Index]			
				, ddips.[avg_fragmentation_in_percent] AS [Average Fragmentation (%)]								
				, ''['' + OBJECT_SCHEMA_NAME(ddips.[object_id], DB_ID(''',
                    @database,
                    ''')) + ''].['' + OBJECT_NAME(ddips.[object_id], DB_ID(''',
                    @database,
                    ''')) + '']''
				, case 
					when (SELECT max(partition_number) FROM sys.partitions ips WHERE ips.index_id = i.index_id and ips.[object_id] = i.[object_id])>1 then 1 
					else 0 
					end AS ispartitioned
				, ddips.partition_number
				, ISNULL(''',
                    ISNULL(@forceCompression, 'NULL'),
                    ''', p.data_compression_desc)
				, i.type
				, case 
					when isnull(',
                    ISNULL(CAST(@fillFactor AS VARCHAR(5)), 'NULL'),
                    ', fill_factor) = 0 then 100 
					else isnull(',
                    ISNULL(CAST(@fillFactor AS VARCHAR(5)), 'NULL'),
                    ', fill_factor) 
					end as fill_factor
		FROM    sys.dm_db_index_physical_stats(DB_ID(''',
                    @database,
                    '''), ',
                    ISNULL(@objectId, 0),
                    ', ',
                    ISNULL(@index_Id, -1),
                    ', NULL, ''',
                    @scanMode,
                    ''') ddips
				INNER JOIN sys.[indexes] i 
						ON  ddips.[object_id] = i.[object_id]
							AND ddips.[index_id] = i.[index_id]
				INNER JOIN sys.partitions p 
						ON  ddips.[object_id] = p.[object_id] 
							AND ddips.index_id = p.index_id 
							AND ddips.partition_number = p.partition_number
				INNER JOIN (VALUES  
					(0, ',
                    @rebuildHeap,
                    '),
					(1, ',
                    @rebuildClustered,
                    '),
					(2, ',
                    @rebuildNonclustered,
                    '),
					(3, ',
                    @rebuildXML,
                    '),
					(4, ',
                    @rebuildSpatial,
                    '),
					(5, ',
                    @rebuildClColumnstore,
                    '),
					(7, ',
                    @rebuildNcColumnstore,
                    ')) indexTypes(typeId, shouldRebuild)
						ON indexTypes.typeId = i.type and indexTypes.shouldRebuild = 1
		WHERE   ddips.[avg_fragmentation_in_percent] > ',
                    @fragmentation,
                    '
				AND ddips.[page_count] > ',
                    @pages,
                    '
				'
                    + CASE
                          WHEN @schema IS NULL THEN
                              N''
                          ELSE
                              N'AND OBJECT_SCHEMA_NAME(ddips.[object_id], DB_ID(''' + @database + ''')) = ''' + @schema
                              + ''''
                      END + N'
				' +                             CASE
                                                    WHEN @table IS NULL THEN
                                                        N''
                                                    ELSE
                                                        N'AND OBJECT_NAME(ddips.[object_id], DB_ID(''' + @database
                                                        + ''')) =''' + @table + ''''
                                                END + N'
				AND ddips.partition_number BETWEEN ',
                    @partitionFrom,
                    ' AND ',
                    @partitionTo,
                    '
		ORDER BY OBJECT_NAME(ddips.[object_id], DB_ID(''',
                    @database,
                    ''')) ,
				i.[name],
				ddips.partition_number'
                );



    IF @dryRun = 1
        PRINT @indexSql;

    INSERT INTO @indexTable
    (
        indexId,
        indexName,
        fragmentation,
        tableName,
        isPartitioned,
        partitionNumber,
        dataCompression,
        typeId,
        fillFact
    )
    EXEC [sys].[sp_executesql] @indexSql, @indexSqlParams, @database = @database, @schema = @schema, @table = @table, @fragmentation = @fragmentation, @pages = @pages, @fillFactor = @fillFactor, @rebuildHeap = @rebuildHeap, @rebuildClustered = @rebuildClustered, @rebuildNonclustered = @rebuildNonclustered, @rebuildXML = @rebuildXML, @rebuildSpatial = @rebuildSpatial, @rebuildClColumnstore = @rebuildClColumnstore, @rebuildNcColumnstore = @rebuildNcColumnstore, @objectId = @objectId, @indexId = @index_Id, @scanMode = @scanMode, @partitionFrom = @partitionFrom, @partitionTo = @partitionTo, @forceCompression = @forceCompression;



    IF (@scanOnly = 1)
    BEGIN
        SELECT tableName,
            indexName,
            partitionNumber,
            typeId,
            dataCompression,
            fillFact,
            fragmentation
        FROM @indexTable;
        RETURN;
    END;


    DECLARE @indexId INT,
        @indexName NVARCHAR(130),
        @idxFragmentation FLOAT,
        @isPartitioned BIT,
        @partitionNumber INT,
        @tableName NVARCHAR(261),
        @dataCompression VARCHAR(20),
        @typeId TINYINT,
        @fillFact TINYINT;

    DECLARE @baseSQL NVARCHAR(MAX)
        = N'USE [%database%];' + CHAR(13) + CHAR(10)
          + 'ALTER %object% %indexName% %on% %tableName% %operation% %options%',
        @operationSQL NVARCHAR(MAX),
        @optionSQL NVARCHAR(MAX),
        @objectSQL NVARCHAR(200),
        @sql NVARCHAR(MAX),
        @errorMessage NVARCHAR(MAX) = '',
        @errored BIT = 0;

    DECLARE @baseMsg VARCHAR(MAX)
        = '--Table: %tableName% ; Index: %indexName% Operation: %operation% ;Fragmentation: %idxFragmentation% ; Options: %options%',
        @msg VARCHAR(MAX);

    DECLARE @CheckTypesSql NVARCHAR(MAX)
        = '
												use [' + @database
          + '];
												if (select type from sys.indexes i where i.object_id = object_id(@TableName) and i.index_id = @indexId) in (0, 1)
												begin
													select @ReturnValue = count(1)
													from			
															sys.columns c 
															join sys.types t on c.user_type_id = t.user_type_id

													where	c.object_id = object_id(@TableName) 
															AND t.name in (''text'', ''ntext'', ''image'', ''FILESTREAM'')
												end
												else
												begin
													select @ReturnValue = count(1)
													from	
															sys.index_columns ic 
															join sys.columns c on ic.column_id = c.column_id and ic.object_id = c.object_id
															join sys.types t on c.user_type_id = t.user_type_id

													where	ic.object_id = object_id(@TableName)
															AND ic.index_id = @IndexId 
															AND t.name in (''text'', ''ntext'', ''image'', ''FILESTREAM'')
												end',
        @CheckTypesParams NVARCHAR(MAX) = '@TableName NVARCHAR(261), @indexId INT, @ReturnValue INT OUTPUT',
        @ReturnValue INT;

    DECLARE @statementTable TABLE
    (
        tableName NVARCHAR(261),
        sqlStatement NVARCHAR(MAX),
        logMessage NVARCHAR(MAX)
    );
    DECLARE @EnqueueSql NVARCHAR(MAX)
        = 'exec pp.EnqueueTask @TaskSql = @TaskSql, @TaskGroupId = @TaskGroupId',
        @EnqueuePar NVARCHAR(MAX) = '@TaskSql NVARCHAR(MAX), @TaskGroupId BIGINT',
        @WaitSql NVARCHAR(MAX) = 'exec pp.WaitForTaskGroup  @TaskGroupId = @TaskGroupId, @TimeoutPerTask = NULL, @TimeoutJob = NULL',
        @WaitPar NVARCHAR(MAX) = '@TaskGroupId BIGINT',
        @GetTaskGroupIdSql NVARCHAR(MAX) = 'exec  pp.GetTaskGroupId @TaskGroupId = @TaskGroupId OUTPUT',
        @GetTaskGroupIdPar NVARCHAR(MAX) = '@TaskGroupId BIGINT OUTPUT',
        @TaskGroupId BIGINT;

    IF (UPPER(@runInParallel) <> 'NO')
    BEGIN
        EXEC [sys].[sp_executesql] @GetTaskGroupIdSql,
            @GetTaskGroupIdPar,
            @TaskGroupId = @TaskGroupId OUTPUT;
        PRINT 'TaskGroupId = ' + CAST(@TaskGroupId AS NVARCHAR(20));
    END;

    WHILE (1 = 1)
    BEGIN
        SELECT @optionSQL = NULL,
            @operationSQL = NULL,
            @optionSQL = '',
            @sql = NULL,
            @msg = NULL;


        SELECT TOP (1)
            @indexId = [it].[indexId],
            @indexName = [it].[indexName],
            @idxFragmentation = [it].[fragmentation],
            @tableName = [it].[tableName],
            @isPartitioned = [it].[isPartitioned],
            @partitionNumber = [it].[partitionNumber],
            @dataCompression = [it].[dataCompression],
            @typeId = [it].[typeId],
            @fillFact = [it].[fillFact]
        FROM @indexTable it
        ORDER BY [it].[tableName],
            [it].[indexName],
            [it].[partitionNumber];
        IF (@@ROWCOUNT = 0)
        BEGIN
            BREAK;
        END;
        WITH cte
        AS (SELECT TOP (1)
                *
            FROM @indexTable it
            ORDER BY [it].[tableName],
                [it].[indexName],
                [it].[partitionNumber]
           )
        DELETE TOP (1)
        FROM cte;

        IF (@typeId = 0)
        BEGIN
            SET @objectSQL = 'TABLE';
        END;

        IF (@typeId IN ( 1, 2, 3, 4, 5, 6 ))
        BEGIN
            SET @objectSQL = 'INDEX';
        END;

        IF (@idxFragmentation < @reorganizeLimit AND @typeId <> 0)
        BEGIN
            SET @operationSQL = N' REORGANIZE ';
        END;
        IF (@idxFragmentation >= @reorganizeLimit OR @typeId = 0)
        BEGIN
            SET @operationSQL = N' REBUILD ';
            SET @optionSQL
                = N'WITH(DATA_COMPRESSION = ' + @dataCompression + ', MAXDOP = '
                  + CAST(ISNULL(@maxdop, 0) AS NVARCHAR(3)) + ', SORT_IN_TEMPDB = ' + CASE @sortInTempDb
                                                                                          WHEN 1 THEN
                                                                                              N'ON'
                                                                                          ELSE
                                                                                              'OFF'
                                                                                      END;

            IF @isEnterprise = 1
               AND @online = 1
            BEGIN
                EXEC [sys].[sp_executesql] @CheckTypesSql,
                    @CheckTypesParams,
                    @TableName = @tableName,
                    @IndexId = @indexId,
                    @ReturnValue = @ReturnValue OUTPUT;
                IF @ReturnValue = 0
                BEGIN
                    SET @optionSQL = @optionSQL + N', ONLINE = ON';
                    IF @waitAtLowPriorityDuration IS NOT NULL
                       OR @waitAtLowPriorityAbort IS NOT NULL
                        SET @optionSQL += CONCAT(
                                                    ' ( WAIT_AT_LOW_PRIORITY ( MAX_DURATION = ',
                                                    ISNULL(@waitAtLowPriorityDuration, 0),
                                                    ' MINUTES, ABORT_AFTER_WAIT = ',
                                                    @waitAtLowPriorityAbort,
                                                    ') )'
                                                );
                END;
            END;
            IF (@isPartitioned = 0)
            BEGIN
                SET @optionSQL = @optionSQL + N', FILLFACTOR = ' + CAST(@fillFact AS NVARCHAR(3));
            END;

            SET @optionSQL = @optionSQL + ')';

        END;

        IF (@isPartitioned = 1)
        BEGIN
            SET @operationSQL = @operationSQL + N'PARTITION = ' + CAST(@partitionNumber AS NVARCHAR(5));
        END;

        SET @sql = @baseSQL;
        SET @sql = REPLACE(@sql, N'%object%', @objectSQL);
        SET @sql = REPLACE(@sql, N'%operation%', @operationSQL);
        SET @sql = REPLACE(@sql, N'%indexName%', ISNULL(@indexName, ''));
        SET @sql = REPLACE(@sql, N'%tableName%', @tableName);
        SET @sql = REPLACE(@sql, N'%options%', @optionSQL);
        SET @sql = REPLACE(   @sql,
                              N'%on%',
                              CASE
                                  WHEN @indexName IS NULL THEN
                                      ''
                                  ELSE
                                      'ON'
                              END
                          );
        SET @sql = REPLACE(@sql, N'%database%', @database);

        SET @msg = @baseMsg;
        SET @msg = REPLACE(@msg, N'%operation%', @operationSQL);
        SET @msg = REPLACE(@msg, N'%object%', @objectSQL);
        SET @msg = REPLACE(@msg, N'%indexName%', ISNULL(@indexName, 'HEAP'));
        SET @msg = REPLACE(@msg, N'%tableName%', @tableName);
        SET @msg = REPLACE(@msg, N'%idxFragmentation%', CAST(@idxFragmentation AS VARCHAR(20)));
        SET @msg = REPLACE(@msg, N'%options%', @optionSQL);

        IF (UPPER(@runInParallel) = 'NO')
        BEGIN
            PRINT @msg;
            IF (@dryRun = 1)
            BEGIN
                PRINT @sql;
                PRINT 'GO';
                PRINT '';
                CONTINUE;
            END;
            BEGIN TRY
                EXEC [sys].[sp_executesql] @sql;
                SET @sql = CONCAT('WAITFOR DELAY ''', @waitBetweenRebuilds, '''');
                EXEC [sys].[sp_executesql] @sql;
            END TRY
            BEGIN CATCH
                SET @errorMessage = ERROR_MESSAGE();
                SET @errored = 1;
                RAISERROR(@errorMessage, 8, 8);
            END CATCH;
        END;

        IF (UPPER(@runInParallel) = 'ALL')
        BEGIN
            PRINT '--Enqueuing task: ';
            PRINT @msg;
            IF (@dryRun = 1)
            BEGIN
                PRINT @sql;
                PRINT '';
                CONTINUE;
            END;
            EXEC [sys].[sp_executesql] @EnqueueSql,
                @EnqueuePar,
                @TaskGroupId = @TaskGroupId,
                @TaskSql = @sql;
        END;

        IF (UPPER(@runInParallel) = 'TABLES')
        BEGIN
            INSERT INTO @statementTable
            (
                tableName,
                sqlStatement,
                logMessage
            )
            VALUES
            (@tableName,
                @sql,
                @msg
            );
        END;


    END;

    IF (UPPER(@runInParallel) = 'TABLES')
    BEGIN
        WHILE 1 = 1
        BEGIN
            ;
            WITH cte
            AS (SELECT [Results].[tableName],
                    STUFF(
                             (
                                 SELECT ';' + CHAR(13) + CHAR(10) + sqlStatement
                                 FROM @statementTable
                                 WHERE (tableName = [Results].[tableName])
                                 FOR XML PATH(''), TYPE
                             ).value('(./text())[1]', 'VARCHAR(MAX)'),
                             1,
                             2,
                             ''
                         ) AS statements,
                    STUFF(
                             (
                                 SELECT CHAR(13) + CHAR(10) + logMessage
                                 FROM @statementTable
                                 WHERE (tableName = [Results].[tableName])
                                 FOR XML PATH(''), TYPE
                             ).value('(./text())[1]', 'VARCHAR(MAX)'),
                             1,
                             2,
                             ''
                         ) AS Msg
                FROM @statementTable Results
                GROUP BY [Results].[tableName]
               )
            SELECT @sql = [cte].[statements],
                @msg = [cte].[Msg],
                @tableName = [cte].[tableName]
            FROM cte;
            DELETE FROM @statementTable
            WHERE @tableName = tableName;

            IF (@@ROWCOUNT = 0)
            BEGIN
                BREAK;
            END;
            PRINT '--Enqueuing task: ';
            PRINT @msg;
            IF (@dryRun = 1)
            BEGIN
                PRINT @sql;
                PRINT 'GO';
                PRINT '';
                CONTINUE;
            END;
            EXEC [sys].[sp_executesql] @EnqueueSql,
                @EnqueuePar,
                @TaskGroupId = @TaskGroupId,
                @TaskSql = @sql;

        END;
    END;



    IF (UPPER(@runInParallel) <> 'NO')
    BEGIN
        IF (@dryRun = 1)
        BEGIN
            PRINT '--' + @WaitSql + '   ' + CAST(@TaskGroupId AS NVARCHAR(50));
            PRINT '';
        END;
        ELSE
        BEGIN
            EXEC [sys].[sp_executesql] @WaitSql,
                @WaitPar,
                @TaskGroupId = @TaskGroupId;
        END;
    END;


    IF @errored = 1
    BEGIN
        RAISERROR('Errors encountered during rebuild. Check warnings above.', 16, 16);

    END;






GO
