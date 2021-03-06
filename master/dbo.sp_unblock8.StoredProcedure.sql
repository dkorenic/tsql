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
/****** Object:  StoredProcedure [dbo].[sp_unblock8]    Script Date: 11/2/2017 7:22:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_unblock8]
   @login NVARCHAR(128) = NULL
 , @spid INT = NULL
 , @executer NVARCHAR(128) = NULL
 , @database NVARCHAR(128) = NULL
 , @hostname NVARCHAR(256) = NULL
 , @debug BIT = 1
AS
DECLARE @sql NVARCHAR(MAX);



WHILE 1 = 1
BEGIN
   IF OBJECT_ID('tempdb..#temp') IS NOT NULL
	 DROP TABLE #temp;
   WITH  cte
		AS (
		    SELECT
			   [des].[login_name] AS [Login]
			 , CASE WHEN [des].[program_name] LIKE 'SQLAgent - TSQL JobStep%'
				   THEN CONCAT([sj].[name], '; ', REPLACE(SUBSTRING([des].[program_name], 67, 20), ')', ''))
				   WHEN [des].[program_name] LIKE 'Microsoft SQL Server Management Studio%' THEN CONCAT([des].[host_name], ' - SSMS')
				   WHEN [des].[program_name] LIKE 'SQL Monitor%' THEN 'SQL Monitor'
				   WHEN [des].[program_name] LIKE 'DatabaseMail%' THEN 'Db Mail'
				   WHEN [des].[program_name] LIKE 'Microsoft SQL Server' THEN CONCAT([des].[host_name], ' - SS')
				   ELSE [des].[program_name]
			   END AS [Executer]
			 , [der].[command] AS [Command]
			 , (
			    SELECT TOP 1
				    SUBSTRING(dest.text, [der].[statement_start_offset] / 2 + 1,
						    ((CASE WHEN [der].[statement_end_offset] = -1 THEN (LEN(CONVERT(NVARCHAR(MAX), dest.text)) * 2)
								 ELSE [der].[statement_end_offset]
							 END) - [der].[statement_start_offset]) / 2 + 1)
			   ) AS [ExecutingStatement]
			 , [dest].[text] AS [BatchText]
			 , [der].[session_id] AS [spid]
			 , [der].[blocking_session_id] [BlockingSpid]
			 , DB_NAME([der].[database_id]) AS [Database]
			 , [des].[host_name] AS [Hostname]
			 , [der].[start_time] AS [CommandStartTime]
			FROM
			   [sys].[dm_exec_requests] der WITH (NOLOCK)
			   LEFT JOIN [sys].[dm_exec_connections] dec WITH (NOLOCK)
				 ON [der].[session_id] = [dec].[session_id]
			   LEFT JOIN [sys].[dm_exec_sessions] des WITH (NOLOCK)
				 ON [des].[session_id] = [der].[session_id]
			   OUTER APPLY [sys].[dm_exec_sql_text]([der].[sql_handle]) AS dest
			   LEFT JOIN [msdb].[dbo].[sysjobs] sj
				 ON SUBSTRING((CAST([des].[program_name] AS VARCHAR(75))), 32, 32) = SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 7, 2)
				    + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 5, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 3, 2)
				    + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 1, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 12, 2)
				    + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 10, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 17, 2)
				    + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 15, 2) + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 20, 4)
				    + SUBSTRING((CAST([sj].[job_id] AS VARCHAR(36))), 25, 12)
		   )
	 SELECT
		  *
	    INTO
		  #temp
	    FROM
		  cte
	    WHERE
		  1 = 1
		  AND [cte].[spid] <> @@spid
		  AND (@spid = [cte].[spid]
			  OR @spid IS NULL)
		  AND [cte].[BlockingSpid] > 0
		  AND ([cte].[Login] LIKE '%' + @login + '%'
			  OR @login IS NULL)
		  AND ([cte].[Database] LIKE '%' + @database + '%'
			  OR @database IS NULL)
		  AND ([cte].[Hostname] LIKE '%' + @hostname + '%'
			  OR @hostname IS NULL)
		  AND ([cte].[Executer] LIKE '%' + @executer + '%'
			  OR @executer IS NULL);
   IF @@rowcount = 0
	 BREAK;


   SET @sql = ( SELECT DISTINCT
				 CONCAT('
				 BEGIN TRY 
				    KILL ', [BlockingSpid], '
				  END TRY
				  BEGIN CATCH
				  END CATCH')
			   FROM
				 #temp
   FOR
	 XML PATH('')
	   , TYPE).value('.', 'nvarchar(max)');
   IF @debug = 1
   BEGIN
	 SELECT
		  *
	    FROM
		  #temp;
	 PRINT @sql;
	 BREAK;
   END;
   ELSE
   BEGIN
	 EXEC (@sql);
   END;


END;


GO
