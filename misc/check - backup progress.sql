SELECT r.session_id                                                                    AS SPID
     , r.command
     , a.text                                                                          AS Query
     , r.start_time
     , r.percent_complete
     , CAST(DATEADD(SECOND, r.estimated_completion_time / 1000, GETDATE()) AS time(0)) AS estimated_completion_time
FROM sys.dm_exec_requests                          AS r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS a
WHERE r.command LIKE 'BACKUP%'
      OR r.command LIKE 'RESTORE%';