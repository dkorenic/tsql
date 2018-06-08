USE msdb;
GO


SELECT *
FROM dbo.sysjobsteps
WHERE subsystem = 'TSQL'
      AND database_name IN (
                               SELECT database_name FROM master.sys.availability_databases_cluster
                           )
ORDER BY database_name, job_id, step_id;


DECLARE @command nvarchar(MAX)
      , @job_id  uniqueidentifier
      , @step_id int;

SELECT TOP 1
    @job_id  = job_id
  , @step_id = step_id
  , @command = CONCAT('IF sys.fn_hadr_is_primary_replica(DB_NAME()) = 1
BEGIN
', command, '
END')
FROM dbo.sysjobsteps
WHERE subsystem = 'TSQL'
      AND database_name IN (
                               SELECT database_name FROM master.sys.availability_databases_cluster
                           )
      AND command NOT LIKE '%fn_hadr_is_primary_replica%'
ORDER BY database_name, job_id, step_id;

IF @@ROWCOUNT > 0
BEGIN

    EXEC dbo.sp_update_jobstep @job_id = @job_id, @step_id = @step_id, @command = @command;

    SELECT *
    FROM dbo.sysjobsteps
    WHERE subsystem = 'TSQL'
          AND database_name IN (
                                   SELECT database_name FROM master.sys.availability_databases_cluster
                               )
    ORDER BY database_name, job_id, step_id;
END;

