USE msdb
GO


SELECT p.name
     , j.job_id
FROM dbo.sysjobs               AS j
    JOIN sys.server_principals AS p
        ON p.sid = j.owner_sid
WHERE p.name != 'sa';

DECLARE @job_id uniqueidentifier;

SELECT TOP 1
    @job_id = j.job_id
FROM dbo.sysjobs               AS j
    JOIN sys.server_principals AS p
        ON p.sid = j.owner_sid
WHERE p.name != 'sa';

IF @@ROWCOUNT > 0
BEGIN

    EXEC dbo.sp_update_job @job_id = @job_id, @owner_login_name = 'sa';

    SELECT p.name
         , j.job_id
    FROM dbo.sysjobs               AS j
        JOIN sys.server_principals AS p
            ON p.sid = j.owner_sid
    WHERE p.name != 'sa';

END;
