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
/****** Object:  StoredProcedure [dbo].[sp_kill]    Script Date: 11/2/2017 7:22:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [dbo].[sp_kill]
		@dbname			sysname = null,
		@login			nvarchar(128) = null,
		@hostname		nvarchar(256) = null,
		@programName	nvarchar(256) = null,
		@killAll		bit = 0,
		@safety			tinyint	= 1 --needs to be 8 to kill. used as a safety measure against accidental kills. 
AS

	if @dbname is null and @login is null and @hostname is null and @programname is null and @killAll <> 1
	begin
		raiserror('If you want to kill all processes you have to specify @killAll param',16,16)
		return;
	end

	declare @sql nvarchar(max) = 

	'
	DECLARE @spid int
	SELECT 
			@spid = min(spid) 
		from 
			master.dbo.sysprocesses 
		where 
			spid > 50
			and spid not in(@@spid, ' + cast(@@spid as nvarchar(20)) + ')
	' 
	IF @dbname is not null set @sql = @sql + ' and dbid = db_id(''' + @dbname + ''')'
	IF @login is not null set @sql = @sql + ' and loginame like ''%' + @login + '%'''
	IF @hostname is not null set @sql = @sql + ' and hostname like ''%' + @hostname + '%'''
	IF @programName is not null set @sql = @sql + ' and program_name like ''%' + @programName + '%'''

	set @sql = @sql + '
	WHILE @spid IS NOT NULL
	BEGIN
	--print @spid
	EXECUTE (''KILL '' + @spid)
	SELECT 
			@spid = min(spid) 
		from 
			master.dbo.sysprocesses 
		where 
			spid > @spid
			and spid > 50
			and spid not in(@@spid, ' + cast(@@spid as nvarchar(20)) + ')
	' 
	IF @dbname is not null set @sql = @sql + ' and dbid = db_id(''' + @dbname + ''')'
	IF @login is not null set @sql = @sql + ' and loginame like ''%' + @login + '%'''
	IF @hostname is not null set @sql = @sql + ' and hostname like ''%' + @hostname + '%'''
	IF @programName is not null set @sql = @sql + ' and program_name like ''%' + @programName + '%'''
	set @sql = @sql + '
	END
	'
 
	print @sql
	if(@safety = 8) 	exec (@sql)

GO
