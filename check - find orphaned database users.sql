EXEC sys.sp_MSforeachdb 'USE [?];
SELECT u.name
     , l.name
	 , CONCAT(''USE [?]; DROP USER '', QUOTENAME(u.name))
FROM [?].sys.database_principals        AS u
    LEFT JOIN [?].sys.server_principals AS l
        ON l.sid = u.sid
WHERE u.type = ''G'' AND l.name IS NULL;'


