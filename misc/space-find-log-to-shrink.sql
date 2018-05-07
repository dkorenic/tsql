DECLARE @result table
(
    Database_Name varchar(150)
  , Log_Size float
  , Log_Space float
  , Status varchar(100)
);

INSERT INTO @result
EXEC ('DBCC sqlperf(LOGSPACE) WITH NO_INFOMSGS');

-- only return for the DB in context, rounding it 
SELECT Database_Name
     , ROUND(Log_Size, 2)  AS Log_Size
     , ROUND(Log_Space, 2) AS Log_Space
     , Status
	 , ROUND((1.0 - Log_Space / 100.0), 2) * Log_Size Log_Free
FROM @result
ORDER BY Log_Free DESC;
