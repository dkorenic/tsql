--UPDATE tempdb.dbo.temp_for_index_compression SET status = '0' WHERE data_compression != 'NONE'

--DELETE FROM tempdb.dbo.temp_for_index_compression WHERE status = '0'

SELECT *
     , page_count * 8 * 1024 / 1024 / 1024 AS mbytes
FROM tempdb.dbo.temp_for_index_compression
--WHERE data_compression = 'NONE'
ORDER BY status DESC
       , IIF(status = '', 1, 1) * page_count;


-- exec sp_who8 


