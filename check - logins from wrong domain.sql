WITH s AS (
    SELECT LEFT(name, CHARINDEX('\', name) - 1) AS domain
         , *
    FROM sys.server_principals
    WHERE CHARINDEX('\', name) > 0
          AND type IN ( 'U', 'G' )
)
SELECT *
FROM s
WHERE
 s.domain NOT IN (default_domain(), 'NT SERVICE', 'NT AUTHORITY', 'HATTRICK');
 