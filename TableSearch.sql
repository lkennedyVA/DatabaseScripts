SELECT DISTINCT
    s.name AS SchemaName,
    concat(s.name,'.',t.name) AS TableName,
    c.name AS ColumnName,
    tp.name AS DataType,
    c.max_length AS MaxLength,
    CASE 
        WHEN c.is_nullable = 1 THEN 'NULL'
        ELSE 'NOT NULL'
    END AS Nullability
FROM 
    sys.columns c
INNER JOIN 
    sys.tables t ON c.object_id = t.object_id
INNER JOIN 
    sys.types tp ON c.user_type_id = tp.user_type_id
INNER JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    tp.name = 'smallint'
    AND c.name LIKE '%batch%id%' 
ORDER BY 
   SchemaName, TableName
