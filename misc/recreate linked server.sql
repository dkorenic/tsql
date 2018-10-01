:setvar name PSKPROD
:setvar server PSK-M-SQL02
:setvar user dss
:setvar pass V0dBYa(bIRlwjFL

:SETVAR IsSqlCmdEnabled "True"

GO
SET NOEXEC OFF
IF N'$(IsSqlCmdEnabled)' NOT LIKE N'True'
BEGIN
    PRINT N'SQLCMD mode must be enabled to successfully execute this script.';
    PRINT N'Disabling script execution.';
    SET NOEXEC ON;
END;
GO

USE [master]
GO

EXEC master.dbo.sp_dropserver @server = N'$(name)', @droplogins='droplogins'
GO

/****** Object:  LinkedServer [$(name)]    Script Date: 2018-09-28 10:20:42 ******/
EXEC master.dbo.sp_addlinkedserver @server = N'$(name)', @srvproduct=N'', @provider=N'SQLNCLI', @provstr=N'Server=$(server);User ID=$(user)'
 /* For security reasons the linked server remote logins password is changed with ######## */
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'$(name)',@useself=N'False',@locallogin=NULL,@rmtuser=N'$(user)',@rmtpassword='$(pass)'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'collation compatible', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'data access', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'dist', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'pub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'rpc', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'rpc out', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'sub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'connect timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'collation name', @optvalue=NULL
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'lazy schema validation', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'query timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'use remote collation', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'$(name)', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO


