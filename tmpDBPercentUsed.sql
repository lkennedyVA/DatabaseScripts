sp_whoisactive

USE tempdb;
GO

SELECT 
    DB_NAME(database_id) AS DatabaseName,
    type_desc AS FileType,
    name AS LogicalFileName,
    size * 8 / 1024 AS SizeMB,        -- Size in MB
    max_size * 8 / 1024 AS MaxSizeMB, -- Max Size in MB (-1 means unlimited)
    growth * 8 / 1024 AS GrowthMB,    -- Growth in MB
    CAST(FILEPROPERTY(name, 'SpaceUsed') * 100.0 / size AS DECIMAL(5, 2)) AS PercentUsed
FROM sys.master_files
WHERE database_id = DB_ID('tempdb')
  --AND type_desc = 'LOG';