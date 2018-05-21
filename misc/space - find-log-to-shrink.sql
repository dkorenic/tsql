DECLARE @result table
(
    Database_Name varchar(150)
  , Log_Size_MB float
  , Log_Used_Percent float
  , Status varchar(100)
);

INSERT INTO @result
EXEC ('DBCC sqlperf(LOGSPACE) WITH NO_INFOMSGS');

SELECT Database_Name
     , ROUND(Log_Size_MB, 2)  AS Log_Size_MB
     , ROUND(Log_Used_Percent, 2) AS Log_Space
     , Status
	 , ROUND((1.0 - Log_Used_Percent / 100.0) * Log_Size_MB, 2) Log_Free_Percent
FROM @result
ORDER BY Log_Free_Percent DESC;