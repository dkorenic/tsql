USE master;
GO

IF OBJECT_DEFINITION(OBJECT_ID('dbo.sp_MapServerRoleMembersToDatabaseRole')) IS NOT NULL
    DROP PROCEDURE dbo.sp_MapServerRoleMembersToDatabaseRole;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
-- =============================================
-- Author:		dkorenic
-- Create date: 2019-04-09
-- Description:	Stavlja u navedenu database rolu svakog usera u bazi čiji login je member u navedenoj server roli. 
--				Npr, uzme sve loginove iz [members] serverske role, po potrebi kreira istoimene usere na bazi ako ne postoje i doda usera u [db_monitor] rolu ako postoji.
-- =============================================
CREATE PROCEDURE dbo.sp_MapServerRoleMembersToDatabaseRole
    @serverRole sysname,
    @databaseRole sysname,
    @loginLike sysname = '%%',
    @createUsers BIT = 1,
    @errorServerRoleNotExistsSeverity TINYINT = 10,
    @errorDatabaseRoleNotExistsSeverity TINYINT = 10
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS
    (
        SELECT 1 / 0
        FROM sys.server_principals AS sr
        WHERE sr.type = 'R'
              AND sr.name = @serverRole
    )
    BEGIN
        RAISERROR(N'Server role [%s] does''t exist.', @errorServerRoleNotExistsSeverity, 1, @serverRole);
        RETURN;
    END;

    IF NOT EXISTS
    (
        SELECT 1 / 0
        FROM sys.database_principals AS dr
        WHERE dr.type = 'R'
              AND dr.name = @databaseRole
    )
    BEGIN
        DECLARE @dbname sysname = DB_NAME();
        RAISERROR(
                     N'Database role [%s] does''t exist in database [%s].',
                     @errorDatabaseRoleNotExistsSeverity,
                     1,
                     @serverRole,
                     @dbname
                 );
        RETURN;
    END;



    IF OBJECT_ID('tempdb..#sqls') IS NOT NULL
        EXEC ('DROP TABLE #sqls');
    CREATE TABLE #sqls
    (
        id INT IDENTITY,
        dbname sysname,
        sqlCreateUser NVARCHAR(MAX),
        sqlAddMember NVARCHAR(MAX)
    );

    WITH sr
    AS (SELECT sr.name COLLATE DATABASE_DEFAULT AS server_role_name,
               sr.principal_id AS server_role_id
        FROM sys.server_principals AS sr
        WHERE sr.type = 'R'
              AND sr.name = @serverRole),
         l
    AS (SELECT sp.name COLLATE DATABASE_DEFAULT AS login_name,
               sp.principal_id AS login_id,
               sp.sid AS login_sid
        FROM sys.server_principals AS sp
        WHERE sp.type IN ( 'S', 'U' )
              AND sp.name LIKE @loginLike),
         srm
    AS (SELECT sr.server_role_name,
               l.login_name,
               l.login_sid
        FROM sr
            JOIN sys.server_role_members AS srm
                ON srm.role_principal_id = sr.server_role_id
            JOIN l
                ON l.login_id = srm.member_principal_id),
         u
    AS (SELECT u.name AS user_name,
               u.sid AS user_sid,
               u.principal_id AS user_id,
               u.type AS user_type
        FROM sys.database_principals AS u
        WHERE u.type IN ( 'U', 'S' )),
         dr
    AS (SELECT dr.name AS role_name,
               dr.principal_id AS role_id,
               dr.type AS role_type
        FROM sys.database_principals AS dr
        WHERE dr.type = 'R'
              AND dr.name = @databaseRole),
         --, drm AS (SELECT dr.role_name, u.user_name, u.user_sid FROM dr JOIN sys.database_role_members AS drm ON drm.role_principal_id = dr.role_id JOIN u ON u.user_id = drm.member_principal_id)
         uwr
    AS (SELECT u.user_name,
               u.user_sid,
               dr.role_name
        FROM u
            LEFT JOIN(sys.database_role_members AS drm
            JOIN dr
                ON drm.role_principal_id = dr.role_id)
                ON u.user_id = drm.member_principal_id)
    INSERT INTO #sqls
    (
        dbname,
        sqlCreateUser,
        sqlAddMember
    )
    SELECT DB_NAME(),
           --srm.server_role_name,
           --srm.login_name,
           --uwr.user_name,
           --uwr.role_name AS database_role_name,
           IIF(uwr.user_name IS NOT NULL,
               '',
               CONCAT('CREATE USER ', QUOTENAME(srm.login_name), ' FOR LOGIN ', QUOTENAME(srm.login_name), '; ')) AS sqlCreateUser,
           IIF(uwr.role_name IS NOT NULL,
               '',
               CONCAT(
                         'ALTER ROLE ',
                         QUOTENAME(@databaseRole),
                         ' ADD MEMBER ',
                         QUOTENAME(ISNULL(uwr.user_name, srm.login_name)),
                         '; '
                     )) AS sqlAddMember
    FROM srm
        LEFT JOIN uwr
            ON srm.login_sid = uwr.user_sid
	WHERE uwr.role_name IS NULL;


    --SELECT * FROM #sqls;

    DECLARE @id INT = 0,
            @sqlCreateUser NVARCHAR(MAX) = '',
            @sqlAddMember NVARCHAR(MAX) = '';
    WHILE 1 = 1
    BEGIN
        SELECT TOP 1
            @id = id,
            @sqlCreateUser = sqlCreateUser,
            @sqlAddMember = sqlAddMember
        FROM #sqls
        WHERE id > @id
        ORDER BY id;
        IF @@ROWCOUNT = 0
            BREAK;

        IF NULLIF(@sqlCreateUser, '') IS NOT NULL
           AND @createUsers = 1
        BEGIN
            PRINT CONCAT('Executing: ', @sqlCreateUser);
            EXEC (@sqlCreateUser);
            SET @sqlCreateUser = '';
        END;
        IF NULLIF(@sqlAddMember, '') IS NOT NULL
           AND NULLIF(@sqlCreateUser, '') IS NULL
        BEGIN
            PRINT CONCAT('Executing: ', @sqlAddMember);
			EXEC (@sqlAddMember);
        END;

    END;
END;
GO

EXEC sys.sp_MS_marksystemobject N'dbo.sp_MapServerRoleMembersToDatabaseRole';
GO

USE PSK_Accounting;
GO

EXEC dbo.sp_MapServerRoleMembersToDatabaseRole @serverRole = 'monitor', @databaseRole = 'db_monitor';


											   