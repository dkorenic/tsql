USE master;
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID('dbo.sp_block8') IS NULL EXEC('CREATE PROCEDURE dbo.sp_block8 AS PRINT 1')
GO

ALTER PROCEDURE dbo.sp_block8
    @login nvarchar(128) = NULL
  , @spid bigint = NULL
  , @database nvarchar(128) = NULL
  , @hostname nvarchar(256) = NULL
  , @includePlan bit = 0
AS
IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;

IF OBJECT_ID('tempdb..#results') IS NOT NULL DROP TABLE #results;


SELECT des.login_name                     AS Login
     , der.command                        AS Command
     , (
           SELECT TOP 1
               SUBSTRING(
                            dest.text
                          , der.statement_start_offset / 2 + 1
                          , ((CASE
                                  WHEN der.statement_end_offset = -1 THEN
                          (LEN(CONVERT(nvarchar(MAX), dest.text)) * 2)
                                  ELSE
                                      der.statement_end_offset
                              END
                             ) - der.statement_start_offset
                            ) / 2 + 1
                        )
       )                                  AS ExecutingStatement
     , dest.text                          AS [Batch Text]
     , des.session_id                     AS [Session ID]
     , ISNULL(der.blocking_session_id, 0) AS blocking_session_id
     , dec.client_net_address             AS [Client Net Address]
     , der.status                         AS Status
     , DB_NAME(der.database_id)           AS [Database Name]
     , der.total_elapsed_time             AS [Command Total Elapsed Time]
     , der.wait_time                      AS [Command Wait Time]
     , der.cpu_time                       AS [Command Cpu Time]
     --des.total_elapsed_time as [Session Total Elapsed Time],
     --des.cpu_time as [Session Cpu Time],  
     --des.memory_usage as [Pages Used In Memory By Session],
     , der.open_transaction_count         AS [Command Open Transactions]
     , des.open_transaction_count         AS [Session Open Transactions]
     , CASE der.transaction_isolation_level
           WHEN 0 THEN
               'Unspecified'
           WHEN 1 THEN
               'Read Uncommitted'
           WHEN 2 THEN
               'Read Committed'
           WHEN 3 THEN
               'Repeatable Read'
           WHEN 4 THEN
               'Serializable'
           WHEN 5 THEN
               'Snapshot'
       END                                AS [Command Transaction Isolation Level]
     , CASE des.transaction_isolation_level
           WHEN 0 THEN
               'Unspecified'
           WHEN 1 THEN
               'Read Uncommitted'
           WHEN 2 THEN
               'Read Committed'
           WHEN 3 THEN
               'Repeatable Read'
           WHEN 4 THEN
               'Serializable'
           WHEN 5 THEN
               'Snapshot'
       END                                AS [Session Transaction Isolation Level]
     , der.last_wait_type
     , der.wait_resource
     , der.start_time                     AS [Command Start Time]
     , der.reads                          AS [Command Reads]
     , der.writes                         AS [Command Writes]
     , der.logical_reads                  AS [Command Logical Reads]
     , der.plan_handle
     --des.reads as [Session Reads],
     --des.writes as [Session Writes],
     --des.logical_reads as [Session Logical Reads],
     --dec.num_reads as [Connection Packet Reads],
     --dec.num_writes as [Connection Packet Writes],
     , der.lock_timeout                   AS [Command Lock Timeout]
     , des.lock_timeout                   AS [Session Lock Timeout]
     , dec.net_transport
     , dec.auth_scheme
     , der.context_info
     , dec.connect_time                   AS [Connect Time]
     , des.login_time                     AS [Login Time]
     , des.host_name                      AS Hostname
     , des.program_name                   AS Program
INTO #tmp
FROM sys.dm_exec_sessions          AS des WITH (NOLOCK)
    LEFT JOIN sys.dm_exec_requests AS der WITH (NOLOCK)
        ON der.session_id = des.session_id
    LEFT JOIN sys.dm_exec_connections                                AS dec WITH (NOLOCK)
        OUTER APPLY sys.dm_exec_sql_text(dec.most_recent_sql_handle) AS dest
        ON dec.session_id = des.session_id;
WITH CTE AS (
    SELECT sp.[Session ID]                       AS RootBlockingSPID
         , sp.[Session ID]
         , sp.blocking_session_id
         , 0                                     AS nestlevel
         , CAST(sp.[Session ID] AS varchar(MAX)) AS blocking_chain
         , sp.Login
         , sp.Command
         , sp.ExecutingStatement
         , sp.[Batch Text]
         , sp.[Client Net Address]
         , sp.Status
         , sp.[Database Name]
         , sp.[Command Total Elapsed Time]
         , sp.[Command Wait Time]
         , sp.[Command Cpu Time]
         , sp.[Command Open Transactions]
         , sp.[Session Open Transactions]
         , sp.[Command Transaction Isolation Level]
         , sp.[Session Transaction Isolation Level]
         , sp.last_wait_type
         , sp.wait_resource
         , sp.[Command Start Time]
         , sp.[Command Reads]
         , sp.[Command Writes]
         , sp.[Command Logical Reads]
         , sp.[Command Lock Timeout]
         , sp.[Session Lock Timeout]
         , sp.net_transport
         , sp.auth_scheme
         , sp.context_info
         , sp.[Connect Time]
         , sp.[Login Time]
         , sp.Hostname
         , sp.Program
         , sp.plan_handle
    FROM #tmp AS sp
    WHERE sp.blocking_session_id = 0
    UNION ALL
    SELECT CTE.RootBlockingSPID
         , sp.[Session ID]
         , sp.blocking_session_id
         , CTE.nestlevel + 1
         , CTE.blocking_chain + ' <-- ' + CAST(sp.[Session ID] AS varchar(MAX))
         , sp.Login
         , sp.Command
         , sp.ExecutingStatement
         , sp.[Batch Text]
         , sp.[Client Net Address]
         , sp.Status
         , sp.[Database Name]
         , sp.[Command Total Elapsed Time]
         , sp.[Command Wait Time]
         , sp.[Command Cpu Time]
         , sp.[Command Open Transactions]
         , sp.[Session Open Transactions]
         , sp.[Command Transaction Isolation Level]
         , sp.[Session Transaction Isolation Level]
         , sp.last_wait_type
         , sp.wait_resource
         , sp.[Command Start Time]
         , sp.[Command Reads]
         , sp.[Command Writes]
         , sp.[Command Logical Reads]
         , sp.[Command Lock Timeout]
         , sp.[Session Lock Timeout]
         , sp.net_transport
         , sp.auth_scheme
         , sp.context_info
         , sp.[Connect Time]
         , sp.[Login Time]
         , sp.Hostname
         , sp.Program
         , sp.plan_handle
    FROM #tmp AS sp
        INNER JOIN CTE
            ON CTE.[Session ID] = sp.blocking_session_id
)
   , CTE2 AS (
    SELECT CTE.RootBlockingSPID
         , CTE.[Session ID]
         , CTE.blocking_session_id
         , CTE.nestlevel
         , CTE.blocking_chain
         , CTE.Login
         , CTE.Command
         , CTE.ExecutingStatement
         , CTE.[Batch Text]
         , CTE.[Client Net Address]
         , CTE.Status
         , CTE.[Database Name]
         , CTE.[Command Total Elapsed Time]
         , CTE.[Command Wait Time]
         , CTE.[Command Cpu Time]
         , CTE.[Command Open Transactions]
         , CTE.[Session Open Transactions]
         , CTE.[Command Transaction Isolation Level]
         , CTE.[Session Transaction Isolation Level]
         , CTE.last_wait_type
         , CTE.wait_resource
         , CTE.[Command Start Time]
         , CTE.[Command Reads]
         , CTE.[Command Writes]
         , CTE.[Command Logical Reads]
         , CTE.[Command Lock Timeout]
         , CTE.[Session Lock Timeout]
         , CTE.net_transport
         , CTE.auth_scheme
         , CTE.context_info
         , CTE.[Connect Time]
         , CTE.[Login Time]
         , CTE.Hostname
         , CTE.Program
         , CTE.plan_handle
    FROM CTE
    WHERE EXISTS
    (
        SELECT 1 FROM CTE AS CTE2 WHERE CTE2.blocking_session_id = CTE.[Session ID]
    )
          AND CTE.blocking_session_id = 0
    UNION ALL
    SELECT CTE.RootBlockingSPID
         , CTE.[Session ID]
         , CTE.blocking_session_id
         , CTE.nestlevel
         , CTE.blocking_chain
         , CTE.Login
         , CTE.Command
         , CTE.ExecutingStatement
         , CTE.[Batch Text]
         , CTE.[Client Net Address]
         , CTE.Status
         , CTE.[Database Name]
         , CTE.[Command Total Elapsed Time]
         , CTE.[Command Wait Time]
         , CTE.[Command Cpu Time]
         , CTE.[Command Open Transactions]
         , CTE.[Session Open Transactions]
         , CTE.[Command Transaction Isolation Level]
         , CTE.[Session Transaction Isolation Level]
         , CTE.last_wait_type
         , CTE.wait_resource
         , CTE.[Command Start Time]
         , CTE.[Command Reads]
         , CTE.[Command Writes]
         , CTE.[Command Logical Reads]
         , CTE.[Command Lock Timeout]
         , CTE.[Session Lock Timeout]
         , CTE.net_transport
         , CTE.auth_scheme
         , CTE.context_info
         , CTE.[Connect Time]
         , CTE.[Login Time]
         , CTE.Hostname
         , CTE.Program
         , CTE.plan_handle
    FROM CTE
    WHERE CTE.blocking_session_id <> 0
)
SELECT CTE2.[Session ID]
     , CTE2.blocking_chain
     , CTE2.Login
     , CTE2.Command
     , CTE2.ExecutingStatement
     , CTE2.[Batch Text]
     , CTE2.Status
     , CTE2.[Database Name]
     , CTE2.[Command Total Elapsed Time]
     , CTE2.[Command Wait Time]
     , CTE2.[Command Cpu Time]
     , CTE2.[Command Open Transactions]
     , CTE2.[Session Open Transactions]
     , CTE2.[Command Transaction Isolation Level]
     , CTE2.[Session Transaction Isolation Level]
     , CTE2.last_wait_type
     , CTE2.wait_resource
     , CAST('1' AS nvarchar(128)) AS [Wait on DB]
     , CAST('1' AS nvarchar(128)) AS [Wait on Object]
     , CAST('1' AS nvarchar(128)) AS [Wait on Index]
     , CAST(1 AS bigint)          AS [Wait on Partition]
     , CTE2.[Command Start Time]
     , CTE2.[Command Reads]
     , CTE2.[Command Writes]
     , CTE2.[Command Logical Reads]
     , CTE2.[Command Lock Timeout]
     , CTE2.[Session Lock Timeout]
     , CTE2.net_transport
     , CTE2.auth_scheme
     , CTE2.context_info
     , CTE2.[Client Net Address]
     , CTE2.[Connect Time]
     , CTE2.[Login Time]
     , CTE2.Hostname
     , CTE2.Program
     , CTE2.plan_handle
INTO #results
FROM CTE2
ORDER BY CTE2.RootBlockingSPID
       , CTE2.blocking_chain;




DECLARE @objectId     bigint
      , @databaseId   bigint
      , @hobtId       bigint
      , @indexId      bigint
      , @partitionId  bigint
      , @pageId       bigint
      , @fileId       bigint
      , @rowId        bigint
      , @waitType     varchar(6)
      , @DatabaseName nvarchar(128)
      , @ObjectName   nvarchar(128)
      , @IndexName    nvarchar(128)
      , @tmp          nvarchar(256)
      , @sql          nvarchar(1000);

DECLARE @page table
(
    ParentObject varchar(1000)
  , Object nvarchar(1000)
  , Field varchar(1000)
  , VALUE varchar(1000)
);





DECLARE @wait_resource nvarchar(255);
DECLARE c CURSOR FOR
SELECT wait_resource
FROM #results
FOR UPDATE OF [Wait on DB]
            , [Wait on Object]
            , [Wait on Index]
            , [Wait on Partition];
OPEN c;

FETCH NEXT FROM c
INTO @wait_resource;

WHILE @@FETCH_STATUS = 0
BEGIN



    IF (@wait_resource LIKE 'KEY: %')
    BEGIN
        SET @tmp = REPLACE(@wait_resource, 'KEY: ', '');
        SET @databaseId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @tmp = SUBSTRING(@tmp, CHARINDEX(':', @tmp) + 1, LEN(@tmp) - CHARINDEX(':', @tmp));
        SET @hobtId = CAST(SUBSTRING(@tmp, 1, CHARINDEX('(', @tmp) - 1) AS bigint);
        SET @waitType = 'KEY';

        SET @sql
            = CONCAT(
                        '
				USE ['
                      , DB_NAME(@databaseId)
                      , ']
				SELECT
						@objectId = object_id
					  , @indexId = index_id
					  , @partitionId = partition_id
					FROM
						sys.partitions
					WHERE
						hobt_id = @hobtId
			'
                    );
        EXEC sys.sp_executesql @sql
                             , N'@objectId bigint out, @indexId bigint out, @partitionId bigint out, @hobtId bigint'
                             , @objectId = @objectId OUT
                             , @indexId = @indexId OUT
                             , @partitionId = @partitionId OUT
                             , @hobtId = @hobtId;

    END;


    --OBJECT: 30:565577053:0 
    IF (@wait_resource LIKE 'OBJECT: %')
    BEGIN
        SET @tmp = REPLACE(@wait_resource, 'OBJECT: ', '');
        SET @databaseId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @tmp = SUBSTRING(@tmp, CHARINDEX(':', @tmp) + 1, LEN(@tmp) - CHARINDEX(':', @tmp));
        SET @objectId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @waitType = 'OBJECT';
    END;


    --PAGE: 30:1:146 
    IF (@wait_resource LIKE 'PAGE: %')
    BEGIN
        --DBCC PAGE (30,1,146) WITH TABLERESULTS
        SET @tmp = REPLACE(@wait_resource, 'PAGE: ', '');
        SET @databaseId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @tmp = SUBSTRING(@tmp, CHARINDEX(':', @tmp) + 1, LEN(@tmp) - CHARINDEX(':', @tmp));
        SET @fileId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @tmp = SUBSTRING(@tmp, CHARINDEX(':', @tmp) + 1, LEN(@tmp) - CHARINDEX(':', @tmp));
        SET @pageId = CAST(SUBSTRING(@tmp, 1, LEN(@tmp)) AS bigint);


        SET @sql
            = CONCAT(
                        '
				USE ['
                      , DB_NAME(@databaseId)
                      , ']
				DBCC PAGE (@DatabaseId,@FileId,@PageId) WITH TABLERESULTS			
			'
                    );

        INSERT INTO @page
        (
            ParentObject
          , Object
          , Field
          , VALUE
        )
        EXEC sys.sp_executesql @sql
                             , N'@DatabaseId bigint, @FileId bigint, @PageId bigint'
                             , @DatabaseId = @databaseId
                             , @FileId = @fileId
                             , @PageId = @pageId;

        SELECT @objectId = VALUE
        FROM @page
        WHERE Field = 'Metadata: ObjectId';
        SELECT @partitionId = VALUE
        FROM @page
        WHERE Field = 'Metadata: PartitionId';
        SELECT @indexId = VALUE
        FROM @page
        WHERE Field = 'Metadata: IndexId';


        SELECT @waitType = 'PAGE';
    END;

    --RID: 30:1:146:0
    IF (@wait_resource LIKE 'RID: %')
    BEGIN
        SET @tmp = REPLACE(@wait_resource, 'RID: ', '');
        SET @databaseId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @tmp = SUBSTRING(@tmp, CHARINDEX(':', @tmp) + 1, LEN(@tmp) - CHARINDEX(':', @tmp));
        SET @fileId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @tmp = SUBSTRING(@tmp, CHARINDEX(':', @tmp) + 1, LEN(@tmp) - CHARINDEX(':', @tmp));
        SET @pageId = CAST(SUBSTRING(@tmp, 1, CHARINDEX(':', @tmp) - 1) AS bigint);
        SET @tmp = SUBSTRING(@tmp, CHARINDEX(':', @tmp) + 1, LEN(@tmp) - CHARINDEX(':', @tmp));
        SET @rowId = CAST(SUBSTRING(@tmp, 1, LEN(@tmp)) AS bigint);

        SET @sql
            = CONCAT(
                        '
				USE ['
                      , DB_NAME(@databaseId)
                      , ']
				DBCC PAGE (@DatabaseId,@FileId,@PageId, @RowId) WITH TABLERESULTS			
			'
                    );

        INSERT INTO @page
        (
            ParentObject
          , Object
          , Field
          , VALUE
        )
        EXEC sys.sp_executesql @sql
                             , N'@DatabaseId bigint, @FileId bigint, @PageId bigint, @RowId bigint'
                             , @DatabaseId = @databaseId
                             , @FileId = @fileId
                             , @PageId = @pageId
                             , @RowId = @rowId;

        SELECT @objectId = VALUE
        FROM @page
        WHERE Field = 'Metadata: ObjectId';
        SELECT @partitionId = VALUE
        FROM @page
        WHERE Field = 'Metadata: PartitionId';
        SELECT @indexId = VALUE
        FROM @page
        WHERE Field = 'Metadata: IndexId';



        SELECT @waitType = 'RID';
    END;



    SELECT @DatabaseName = DB_NAME(@databaseId)
         , @ObjectName   = OBJECT_NAME(@objectId, @databaseId);

    SET @sql
        = N'USE [' + @DatabaseName
          + ']; SELECT @IndexName = name from sys.indexes where object_id = @ObjectId and index_id = @IndexId';

    EXEC sys.sp_executesql @sql
                         , N'@ObjectId bigint, @IndexId bigint, @IndexName nvarchar(128) OUTPUT'
                         , @IndexId = @indexId
                         , @ObjectId = @objectId
                         , @IndexName = @IndexName OUTPUT;


    UPDATE #results
    SET [Wait on DB] = @DatabaseName
      , [Wait on Object] = @ObjectName
      , [Wait on Index] = @IndexName
      , [Wait on Partition] = @partitionId
    WHERE CURRENT OF c;

    FETCH NEXT FROM c
    INTO @wait_resource;
END;
CLOSE c;
DEALLOCATE c;


SET @sql
    = CONCAT(
                N'SELECT '
              , CASE WHEN @includePlan = 1 THEN N'query_plan ,' END
              , N' 
					  *
 from #results r'
              , CASE WHEN @includePlan = 1 THEN N'
						outer APPLY sys.dm_exec_query_plan(r.plan_handle) eqp ' END
              , N' WHERE 1=1'
              , CASE WHEN @login IS NOT NULL THEN N' AND [Login] like @login ' END
              , CASE WHEN @spid IS NOT NULL THEN N' AND [blocking_chain] like @spid ' END
              , CASE WHEN @database IS NOT NULL THEN N' AND [Database Name] = @database ' END
              , CASE WHEN @hostname IS NOT NULL THEN N' AND [Hostname] like @hostname ' END
              , '
													  ORDER BY blocking_chain'
            );
EXEC sys.sp_executesql @sql
                     , N'@login NVARCHAR(128) = NULL
  , @spid BIGINT = NULL
  , @database NVARCHAR(128) = NULL
  , @hostname NVARCHAR(256) = NULL'
                     , @login = @login
                     , @spid = @spid
                     , @database = @database
                     , @hostname = @hostname;


GO




USE master;
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE type = 'R' AND name = 'sp_whoers')
    CREATE ROLE sp_whoers;
GO
GRANT EXECUTE ON dbo.sp_block8 TO sp_whoers;
GO

USE msdb;
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE type = 'R' AND name = 'sp_whoers')
    CREATE ROLE sp_whoers;
GO
GRANT SELECT ON dbo.sysjobs TO sp_whoers;
GO

