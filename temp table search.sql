-- Query for temporary tables with SMALLINT columns in stored procedures and functions
SELECT 
    o.type_desc AS ObjectType,
    SCHEMA_NAME(o.schema_id) AS SchemaName,
    OBJECT_NAME(o.object_id) AS ObjectName,
    SCHEMA_NAME(o.schema_id) + '.' + OBJECT_NAME(o.object_id) AS FullObjectName,
    'Temporary Table' AS ColumnType,
    'smallint' AS DataType,
    SUBSTRING(m.definition, 
              CHARINDEX('#', m.definition, CHARINDEX('CREATE TABLE', m.definition)), 
              CHARINDEX('(', m.definition, CHARINDEX('#', m.definition, CHARINDEX('CREATE TABLE', m.definition))) 
              - CHARINDEX('#', m.definition, CHARINDEX('CREATE TABLE', m.definition))) AS TempTableName
FROM 
    sys.sql_modules m
JOIN 
    sys.objects o ON m.object_id = o.object_id
WHERE 
    m.definition LIKE '%CREATE TABLE #%'
    AND m.definition LIKE '%smallint%'
    AND m.definition LIKE '%Batch%id%' -- Look for smallint columns with name pattern
    AND o.type IN ('P', 'FN', 'IF', 'TF')  -- P = Procedure, FN = Scalar Function, IF = Inline Table-Valued Function, TF = Table-Valued Function
ORDER BY 
    SchemaName, ObjectName;
