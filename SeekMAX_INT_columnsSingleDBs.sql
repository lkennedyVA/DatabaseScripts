DECLARE @TableName NVARCHAR(128);
DECLARE @SchemaName NVARCHAR(128);
DECLARE @ColumnName NVARCHAR(128);
DECLARE @DataType NVARCHAR(128);
DECLARE @DatabaseName NVARCHAR(128);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @MaxValue INT;
DECLARE @RowIndex INT = 1;
DECLARE @TotalRows INT;

-- Specify the current database name
SET @DatabaseName = DB_NAME();

-- Temporary table to store INT columns along with schema names and data type
DROP TABLE IF EXISTS #IntColumns
CREATE TABLE #IntColumns (
    Id INT IDENTITY(1,1),
    TableName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    DATA_TYPE NVARCHAR(128) 
);

-- Populate the temporary table with all INT columns in the database including schema and data type
INSERT INTO #IntColumns (TableName, SchemaName, ColumnName, DATA_TYPE)
SELECT 
    TABLE_NAME, 
    TABLE_SCHEMA,
    COLUMN_NAME,
    DATA_TYPE
FROM 
    INFORMATION_SCHEMA.COLUMNS 
WHERE 
    DATA_TYPE = 'int'
    AND TABLE_NAME IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE');

-- Get the total number of INT columns to process
SELECT @TotalRows = COUNT(*) FROM #IntColumns;

-- Temporary table to store results
DROP TABLE IF EXISTS #MaxValues
CREATE TABLE #MaxValues (
    DatabaseName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    Data_Type NVARCHAR(128),  
    MaxValue INT
);

-- WHILE loop to process each row in #IntColumns
WHILE @RowIndex <= @TotalRows
BEGIN
    -- Get the current table, schema, column name, and data type based on RowIndex
    SELECT @TableName = TableName, @SchemaName = SchemaName, @ColumnName = ColumnName, @DataType = DATA_TYPE
    FROM #IntColumns
    WHERE Id = @RowIndex;

    -- Check if the table and column exist before running the query
    IF OBJECT_ID(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)) IS NOT NULL
    BEGIN
        -- Build dynamic SQL to get the MAX value for the current column with database and schema names
        SET @SQL = N'SELECT @MaxValue_OUT = MAX(' + QUOTENAME(@ColumnName) + ') FROM ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

        -- Execute the dynamic SQL
        BEGIN TRY
            EXEC sp_executesql @SQL, N'@MaxValue_OUT INT OUTPUT', @MaxValue_OUT = @MaxValue OUTPUT;

            -- Insert the result into the #MaxValues table
            INSERT INTO #MaxValues (DatabaseName, SchemaName, TableName, ColumnName, DATA_TYPE, MaxValue)
            VALUES (@DatabaseName, @SchemaName, @TableName, @ColumnName, @DataType, @MaxValue);
        END TRY
        BEGIN CATCH
            -- Handle any errors gracefully
            PRINT 'Error processing table ' + @SchemaName + '.' + @TableName + ' and column ' + @ColumnName;
        END CATCH;
    END;

    -- Increment the RowIndex
    SET @RowIndex = @RowIndex + 1;
END;

-- Select the results
SELECT * FROM #MaxValues
where MaxValue > 40000000 --40,000,000
order by MaxValue;

-- Drop the temporary tables
DROP TABLE #IntColumns;
DROP TABLE #MaxValues;
