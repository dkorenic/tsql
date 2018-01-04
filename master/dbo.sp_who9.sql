USE master;
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
IF OBJECT_ID('dbo.sp_who9') IS NULL EXEC('CREATE PROCEDURE dbo.sp_who9 AS PRINT 1')
GO

ALTER PROCEDURE dbo.sp_who9
    @login nvarchar(128) = NULL
  , @executer nvarchar(128) = NULL
  , @command nvarchar(32) = NULL
  , @isBlocked bit = NULL
  , @status nvarchar(30) = NULL
  --Background, Running, Runnable, Sleeping, Suspended
  , @spid bigint = NULL
  , @database nvarchar(128) = NULL
  , @hostname nvarchar(256) = NULL
  , @userProcesses bit = 1
  --0 system processes, 1 user processes, null all
  , @includePlan bit = 1
  , @includeChildTasks bit = 0
  , @orderBy nvarchar(MAX) = 'spid '
  , @where nvarchar(MAX) = ''
  , @debug bit = 0
AS
DECLARE @sql    nvarchar(MAX)
      , @params nvarchar(MAX);

DECLARE @productVersion tinyint
    = CONVERT(
                 tinyint
               , SUBSTRING(
                              CONVERT(varchar(128), SERVERPROPERTY('productversion'))
                            , 0
                            , CHARINDEX('.', CONVERT(varchar(128), SERVERPROPERTY('productversion')), 0)
                          )
             );


SET @params
    = '
				@login		nvarchar(128),
				@executer NVARCHAR(128),
				@command	nvarchar(32),
				@isBlocked	bit,
				@status		nvarchar(30), --Background, Running, Runnable, Sleeping, Suspended
				@spid		bigint,
				@database	nvarchar(128),
				@hostname	nvarchar(256),
				@userProcesses bit';


SET @sql
    = CONCAT(
                CONVERT(nvarchar(MAX), '')
              , '
	;with cte as(
			SELECT  
					des.original_login_name AS [Login],
					CASE 
						WHEN des.[program_name] like ''SQLAgent - TSQL JobStep%''
							THEN CONCAT(sj.name, ''; '', REPLACE(SUBSTRING(des.[program_name], 67, 20), '')'', ''''))
						WHEN des.program_name like ''Microsoft SQL Server Management Studio%'' 
							THEN CONCAT(des.[host_name], '' - SSMS'')
						WHEN des.program_name like ''SQL Monitor%''
							THEN ''SQL Monitor''
						WHEN des.program_name like ''DatabaseMail%''
							THEN ''Db Mail''
						WHEN des.program_name like ''Microsoft SQL Server''
							THEN CONCAT(des.host_name, '' - SS'')
						ELSE 
							des.program_name
						END AS [Executer],
			
					der.command AS [Command],
					(SELECT TOP 1 SUBSTRING(dest.text, der.statement_start_offset / 2+1 , 
						  ( (CASE WHEN der.statement_end_offset = -1 
							 THEN (LEN(CONVERT(nvarchar(max),dest.text)) * 2) 
							 ELSE der.statement_end_offset END)  - der.statement_start_offset) / 2+1))  AS [ExecutingStatement],
					dest.text AS [BatchText],		
								
					der.session_id AS [spid],
					der.blocking_session_id [BlockingSpid],
					'
              , IIF(@includeChildTasks = 1, 'dwt.blocking_session_id [TaskBlockingSpid],', '')
              , '
					der.status AS [Status],

					(SELECT COUNT(1) FROM sys.dm_os_tasks  dot1 WHERE der.session_id = dot1.session_id AND der.request_id = dot1.request_id) TaskCount,
					
					'
              , IIF(@includeChildTasks = 1, 'dot.task_state [TaskState],', '')
              , '
					
					'
              , IIF(@includeChildTasks = 1, 'dot.context_switches_count [ContextSwitchesCount],', '')
              , '

					DB_NAME(der.database_id) AS [Database],
					CONCAT(DATEDIFF(DAY, 0, DATEADD(ms,der.total_elapsed_time,0)),''d'', CONVERT(VARCHAR,DATEADD(ms,der.cpu_time,0),114)) as [CommandElapsedTime], 
					'
              , CASE
                    WHEN @includePlan = 1 THEN
                        N'
					--eqp.query_plan [QueryPlan],
					eqp.query_plan [QueryPlan],
					'
                    ELSE
                        ''
                END
              , '
				     der.[wait_type] [CurrentWait],
				     
				     CONCAT(DATEDIFF(DAY, 0, DATEADD(ms,der.[wait_time],0)),''d'', CONVERT(VARCHAR,DATEADD(ms,der.[wait_time],0),114))  [WaitTime],
					der.last_wait_type [LastWait],
					der.wait_resource [WaitResource], 
					'
              , IIF(@includeChildTasks = 1, 'dot.scheduler_id [SchedulerId],', '')
              , '
					'
              , IIF(@includeChildTasks = 1, 'dwt.wait_type [TaskWaitType],', '')
              , '
					'
              , IIF(@includeChildTasks = 1
                  , 'dwt.resource_description + IIF(dwt.ResourceType IS NOT NULL, CONCAT(''('', dwt.ResourceType, '')''), '''') [TaskWaitResource],'
                  , '')
              , '
					CONCAT(convert(numeric(15,2), der.percent_complete, 0), ''%'') PercentComplete, 
				     
					CONCAT(DATEDIFF(DAY, 0, DATEADD(ms,der.cpu_time,0)),''d'', CONVERT(VARCHAR,DATEADD(ms,der.cpu_time,0),114)) as [CommandCpuTime],

					qmg.request_time [MemRequestTime],
					qmg.grant_time [MemGrantTime],
					qmg.requested_memory_kb / 1024.0 [MemRequestMb],
					qmg.granted_memory_kb / 1024.0 [MemGrantMb],
					qmg.required_memory_kb / 1024.0 [MemRequireMb],
					qmg.used_memory_kb / 1024.0 [MemUsedMb],
					qmg.max_used_memory_kb / 1024.0 [MemMaxUsedMb],
					qmg.ideal_memory_kb / 1024.0 [MemIdealMb],
					qmg.query_cost [QueryCost],

					--des.total_elapsed_time as [SessionTotalElapsedTime],
					--des.cpu_time as [SessionCpuTime],  
					des.memory_usage as [PagesUsedInMemoryBySession],
					der.reads as [CommandPhisicalReads],  
					der.writes as [CommandWrites],  
					der.logical_reads as [CommandLogicalReads],
					--des.reads as [SessionReads],
					--des.writes as [SessionWrites],
					--des.logical_reads as [SessionLogicalReads],
					--dec.num_reads as [ConnPacketReads],
					--dec.num_writes as [ConnPacketWrites],

					
					'
              , IIF(@includeChildTasks = 1, 'dot.pending_io_count [PendingIOcount],', '')
              , '
					'
              , IIF(@includeChildTasks = 1, 'dot.pending_io_byte_count [PendingIObyteCount],', '')
              , '
					'
              , IIF(@includeChildTasks = 1, 'dot.pending_io_byte_average [PendingIObyteAverage],', '')
              , '


					case der.transaction_isolation_level 
						when 0 then ''Unspecified''
						when 1 then ''Read Uncommitted''
						when 2 then ''Read Committed''
						when 3 then ''Repeatable Read''
						when 4 then ''Serializable''
						when 5 then ''Snapshot''
						end as [CommandTransactionIsolationLevel],
					case des.transaction_isolation_level 
						when 0 then ''Unspecified''
						when 1 then ''Read Uncommitted''
						when 2 then ''Read Committed''
						when 3 then ''Repeatable Read''
						when 4 then ''Serializable''
						when 5 then ''Snapshot''
						end as [SessionTransactionIsolationLevel],
					des.is_user_process [IsUserProcess],
					der.lock_timeout as [Command Lock Timeout],
					des.lock_timeout as [Session Lock Timeout],
					'
              , CASE WHEN @productVersion > 10 THEN 'des.open_transaction_count' ELSE '-1' END
              , ' as [SessionOpenTransactions],
					der.open_transaction_count as [CommandOpenTransactions],
					der.start_time as [CommandStartTime],  
					dec.net_transport [NetTransport], 
					dec.auth_scheme [AuthScheme],
					der.context_info [Context],
					dec.connect_time as [ConnectTime],
					rgwg.name as [ResourcePool],
					des.login_time AS [LoginTime],
					des.[host_name] AS [Hostname],
					dec.client_net_address [Client Net Address],
					des.[program_name] AS [Program]

			FROM    sys.dm_exec_requests der with(nolock)
					LEFT JOIN sys.dm_exec_connections dec with(nolock)
								   ON der.session_id = dec.session_id
					LEFT JOIN sys.dm_exec_sessions des with(nolock)
								   ON des.session_id = der.session_id
					OUTER APPLY sys.dm_exec_sql_text(sql_handle) AS dest


					LEFT JOIN msdb.dbo.sysjobs sj 
						ON SUBSTRING((CAST(des.[program_name] AS VARCHAR(75))), 32, 32) =	SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 7, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 5, 2)
																						  + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 3, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 1, 2)
																						  + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 12, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 10, 2)
																						  + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 17, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 15, 2)
																						  + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 20, 4) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 25, 12)

					'
              , CASE
                    WHEN @includePlan = 1 THEN
                        N'
					--outer APPLY sys.dm_exec_query_plan(der.plan_handle) AS EQP
					outer APPLY sys.dm_exec_text_query_plan(der.plan_handle,der.statement_start_offset,der.statement_end_offset) AS EQP'
                    ELSE
                        N''
                END
              , N'					
					LEFT JOIN sys.resource_governor_workload_groups rgwg 
						ON des.group_id = rgwg.group_id
					LEFT JOIN sys.[dm_exec_query_memory_grants] qmg
						ON der.session_id = qmg.session_id AND der.request_id = qmg.request_id
				    

				     LEFT JOIN sys.dm_os_tasks dot
					  ON der.session_id = dot.session_id
						AND der.request_id = dot.request_id
						 '
              , IIF(@includeChildTasks = 0, ' AND 1=0', '')
              , '
				    LEFT JOIN (					   
					   SELECT *, 
					    CASE 
						  WHEN PageID = 1 Or PageID % 8088 = 0 THEN ''Is PFS Page''
						  WHEN PageID = 2 Or PageID % 511232 = 0 THEN ''Is GAM Page''
						  WHEN PageID = 3 Or (PageID - 1) % 511232 = 0 THEN ''Is SGAM Page''
						  ELSE NULL --''Is Not PFS, GAM, or SGAM page''
						  END ResourceType
					   FROM(
							 SELECT 
								*, IIF(wait_type Like ''PAGE%LATCH_%'', cast(RIGHT(resource_description, LEN(resource_description) - CHARINDEX('':'', resource_description, CHARINDEX('':'', resource_description, 0) + 1)) as int), NULL) PageID
							 FROM sys.dm_os_waiting_tasks			    
						  ) dwt
				    
				    ) dwt
					  ON der.session_id = dwt.session_id
						AND [dwt].[exec_context_id] = [dot].[exec_context_id]
						 '
              , IIF(@includeChildTasks = 0, ' AND 1=0', '')
              , '
			)
			SELECT * from cte 
			WHERE   
				1=1
				AND spid <> @@spid'
            );

IF (@userProcesses IS NOT NULL) SET @sql = @sql + CHAR(10) + ' AND IsUserProcess = @userProcesses';

IF (@login IS NOT NULL) SET @sql = @sql + CHAR(10) + ' AND [login] LIKE ''%'' + @login + ''%''';

IF (@command IS NOT NULL) SET @sql = @sql + CHAR(10) + ' AND [command] LIKE ''%'' + @command + ''%''';

IF (@isBlocked = 1) SET @sql = @sql + CHAR(10) + ' AND blockingspid > 0';

IF (@isBlocked = 0) SET @sql = @sql + CHAR(10) + ' AND blockingspid = 0';

IF (@status IS NOT NULL) SET @sql = @sql + CHAR(10) + ' AND status  LIKE ''%'' + @status + ''%''';

IF (@spid IS NOT NULL) SET @sql = @sql + CHAR(10) + ' AND spid = @spid';

IF (@database IS NOT NULL) SET @sql = @sql + CHAR(10) + ' AND [Database] LIKE ''%'' + @database + ''%''';

IF (@hostname IS NOT NULL) SET @sql = @sql + CHAR(10) + ' AND Hostname LIKE ''%'' + @hostname + ''%''';

IF @executer IS NOT NULL SET @sql = @sql + ' AND Executer LIKE ''%'' + @executer + ''%''';

SET @sql = @sql + @where;

SET @sql = @sql + CHAR(10) + 'ORDER BY ' + @orderBy;
IF @debug = 1 SELECT @sql;

EXEC sys.sp_executesql @sql
                     , @params
                     , @login = @login
                     , @command = @command
                     , @isBlocked = @isBlocked
                     , @status = @status
                     , @spid = @spid
                     , @database = @database
                     , @hostname = @hostname
                     , @executer = @executer
                     , @userProcesses = @userProcesses;


GO




USE master;
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE type = 'R' AND name = 'sp_whoers')
    CREATE ROLE sp_whoers;
GO
GRANT EXECUTE ON dbo.sp_who9 TO sp_whoers;
GO

USE msdb;
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE type = 'R' AND name = 'sp_whoers')
    CREATE ROLE sp_whoers;
GO
GRANT SELECT ON dbo.sysjobs TO sp_whoers;
GO


