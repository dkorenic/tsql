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
/****** Object:  StoredProcedure [dbo].[sp_lock8]    Script Date: 11/2/2017 7:22:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE procedure [dbo].[sp_lock8]
		@databaseName	nvarchar(128) = null,
		@resourceType	nvarchar(128) = null,--DATABASE,OBJECT,PAGE,KEY...
		@objectName		nvarchar(128) = null,
		@indexName		nvarchar(128) = null,
		@lockMode		nvarchar(2)	  = null --X,S,U,IX,IS,IU...
as
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED




if (object_id('tempdb..#tempRes') is not null) DROP TABLE #tempRes
	
CREATE TABLE #tempRes(
	[resource_type] [nvarchar](60) NOT NULL,
	[databaseName] [nvarchar](128) NULL,
	[objectName]	[nvarchar](128) null,
	[indexName]		[nvarchar](128) null,
	[hobtId] [bigint] NULL,
	[resource_subtype] [nvarchar](60) NOT NULL,
	[resource_description] [nvarchar](256) NOT NULL,
	[NumLocks] [int] NULL,
	[request_mode] [nvarchar](60) NOT NULL,
	[request_type] [nvarchar](60) NOT NULL,
	[request_status] [nvarchar](60) NOT NULL,
	[request_lifetime] [bigint] NOT NULL,
	[request_session_id] [int] NOT NULL,
	[Login] [nvarchar](128) NULL,
	[Command] [nvarchar](32) NULL,
	[ExecutingStatement] [nvarchar](max) NOT NULL,
	[Batch Text] [nvarchar](max) NULL,
	[Session ID] [smallint] NULL,
	[Status] [nvarchar](30) NULL,
	[Command Wait Time] [bigint] NULL,
	[Command Cpu Time] [bigint] NULL,
	[Command Open Transactions] [int] NULL,
	[Command Transaction Isolation Level] [varchar](16) NULL,
	[Session Transaction Isolation Level] [varchar](16) NULL,
	[last_wait_type] [nvarchar](60) NULL,
	[wait_resource] [nvarchar](256) NULL,
	[Command Start Time] [datetime] NULL,
	[Hostname] [nvarchar](128) NULL,
	[Program] [nvarchar](128) NULL
) ON [PRIMARY] 



declare @sql nvarchar(max) = N'
;with cte as (
		select
			resource_type
			, db_name(tl.resource_database_id) as databaseName
			, null as objectName
			, null as indexName
			, tl.resource_associated_entity_id as hobtId
			, tl.resource_subtype
			, tl.resource_description
			, count(tl.resource_type) as NumLocks
			, tl.request_mode
			, tl.request_type
			, tl.request_status
			, tl.request_lifetime
			, tl.request_session_id
			, des.login_name AS [Login]
			, der.command AS [Command]
			, isnull((SELECT TOP 1 SUBSTRING(dest.text, der.statement_start_offset / 2+1 , 
						( (CASE WHEN der.statement_end_offset = -1 
							THEN (LEN(CONVERT(nvarchar(max),dest.text)) * 2) 
							ELSE der.statement_end_offset END)  - der.statement_start_offset) / 2+1)),'''')  AS [ExecutingStatement]
			, dest.text AS [Batch Text]					
			, der.session_id AS [Session ID]
			, der.status AS [Status]
			, der.wait_time as [Command Wait Time] 
			, der.cpu_time as [Command Cpu Time]
			, der.open_transaction_count as [Command Open Transactions]
			, case  der.transaction_isolation_level 
				when 0 then ''Unspecified''
				when 1 then ''Read Uncommitted''
				when 2 then ''Read Committed''
				when 3 then ''Repeatable Read''
				when 4 then ''Serializable''
				when 5 then ''Snapshot''
				end as [Command Transaction Isolation Level]
			, case des.transaction_isolation_level 
				when 0 then ''Unspecified''
				when 1 then ''Read Uncommitted''
				when 2 then ''Read Committed''
				when 3 then ''Repeatable Read''
				when 4 then ''Serializable''
				when 5 then ''Snapshot''
				end as [Session Transaction Isolation Level]
			, der.last_wait_type
			, der.wait_resource  
			, der.start_time as [Command Start Time]  
			, des.[host_name] AS [Hostname]
			, des.[program_name] AS [Program]
		from 
			sys.dm_tran_locks tl	
			left join sys.dm_exec_sessions des
				on tl.request_session_id = des.session_id
			left join sys.dm_exec_requests der
				on tl.request_request_id = der.request_id
			CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS dest 
		where 
			des.session_id != @@SPID
		group by
			resource_type
			, tl.resource_subtype
			, tl.resource_description
			, tl.resource_database_id
			, tl.resource_associated_entity_id		 
			, tl.request_mode
			, tl.request_type
			, tl.request_status
			, tl.request_lifetime
			, tl.request_session_id
			, des.login_name 
			, der.command 
			, dest.text 					
			, der.session_id 
			, der.status 
			, der.wait_time  
			, der.cpu_time 
			, der.open_transaction_count 
			, der.transaction_isolation_level 		
			, des.transaction_isolation_level 	
			, der.last_wait_type
			, der.wait_resource  
			, der.start_time   
			, des.[host_name] 
			, des.[program_name] 
			, der.statement_start_offset
			, der.statement_end_offset)
select * 	
from cte 
	where 
	1=1'
	
if(@databaseName is not null)
	set @sql = @sql + char(13) + char(10) +' AND databaseName LIKE @databaseName '
if(@resourceType is not null)
	set @sql = @sql + char(13) + char(10) +' AND resource_type like @resourceType '
if(@lockMode is not null)
	set @sql = @sql + char(13) + char(10) +' AND request_mode like @lockMode '


insert into #tempRes
exec sp_executesql 
					@sql
					, N'@databaseName nvarchar(128), @resourceType nvarchar(128), @lockMode nvarchar(2)'
					, @databaseName=@databaseName, @resourceType=@resourceType, @lockMode=@lockMode







declare @hobtId		bigint,
		@object		nvarchar(128),
		@index		nvarchar(128),
		@database	nvarchar(128)



declare c cursor local forward_only read_only for select distinct databaseName from #tempRes 
open c


while 1=1
begin
	
	fetch next from c into @database
	if (@@FETCH_STATUS <> 0) break;

	set @sql = N'
	USE [' + @database + N']
	update t 
		set objectName = case resource_Type 
							when ''DATABASE''			THEN @database
							when ''OBJECT'' 			THEN (select name from sys.objects o where o.object_id = hobtId)
							when ''PAGE'' 				THEN (select o.name from sys.partitions p join sys.objects o on p.object_id =  o.object_id and p.hobt_id = hobtId)	
							when ''HOBT''				THEN (select o.name from sys.partitions p join sys.objects o on p.object_id =  o.object_id and p.hobt_id = hobtId)	
							when ''KEY''				THEN (select o.name from sys.partitions p join sys.objects o on p.object_id =  o.object_id and p.hobt_id = hobtId)		
							when ''RID''				THEN (select o.name from sys.partitions p join sys.objects o on p.object_id =  o.object_id and p.hobt_id = hobtId)
							else						 cast(hobtId as nvarchar(128))
							end,
			indexName = (select i.name from sys.partitions p join sys.indexes i on p.object_id =  i.object_id and p.index_id = i.index_id and p.hobt_id = hobtId)
			from #tempRes t
			where databaseName = @database
		'
			
	
	exec sp_executesql @sql, N'@database nvarchar(128)', @database = @database	
end
close c
deallocate c


set @sql = N'select * from #tempRes WHERE 1=1'
if(@objectName is not null)
	set @sql = @sql + char(13) + char(10) +' AND objectName like @objectName '
if(@indexName is not null)
	set @sql = @sql + char(13) + char(10) +' AND indexName like @indexName '
set @sql = @sql + char(13) + char(10) +' order by resource_type desc, login '


exec sp_executesql @sql, N'@objectName nvarchar(128), @indexName nvarchar(128)', @objectName, @indexName


GO
