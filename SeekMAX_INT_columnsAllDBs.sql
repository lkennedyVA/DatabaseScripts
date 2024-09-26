DECLARE @DatabaseName NVARCHAR(128);
DECLARE @TableName NVARCHAR(128);
DECLARE @SchemaName NVARCHAR(128);
DECLARE @ColumnName NVARCHAR(128);
DECLARE @DataType NVARCHAR(128);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @MaxValue INT;
DECLARE @RowIndex INT;
DECLARE @TotalRows INT;
DECLARE @DbIndex INT = 1;  -- Index for the WHILE loop
DECLARE @DbCount INT;

-- Temporary table to store the list of databases
DROP TABLE IF EXISTS #Databases;
CREATE TABLE #Databases (
    Id INT IDENTITY(1,1),
    DatabaseName NVARCHAR(128)
);

-- Populate the temporary table with the list of databases
INSERT INTO #Databases (DatabaseName)
SELECT name
FROM sys.databases
WHERE state_desc = 'ONLINE'  -- Only process online databases
    AND name NOT IN ('master', 'tempdb', 'model', 'msdb');  -- Skip system databases

-- Get the total number of databases to process
SELECT @DbCount = COUNT(*) FROM #Databases;

-- Temporary table to store the final results across all databases
DROP TABLE IF EXISTS #AllMaxValues;
CREATE TABLE #AllMaxValues (
    DatabaseName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    Data_Type NVARCHAR(128),  
    MaxValue INT
);

-- WHILE loop to iterate through databases
WHILE @DbIndex <= @DbCount
BEGIN
    -- Get the current database name based on the index
    SELECT @DatabaseName = DatabaseName
    FROM #Databases
    WHERE Id = @DbIndex;

    -- Reset RowIndex for each database
    SET @RowIndex = 1;

    BEGIN TRY
        -- Temporary table to store INT columns along with schema names and data type for the current database
        DROP TABLE IF EXISTS #IntColumns;
        CREATE TABLE #IntColumns (
            Id INT IDENTITY(1,1),
            TableName NVARCHAR(128),
            SchemaName NVARCHAR(128),
            ColumnName NVARCHAR(128),
            DATA_TYPE NVARCHAR(128)
        );

        -- Populate the temporary table with all INT columns in the current database including schema and data type
        SET @SQL = '
        INSERT INTO #IntColumns (TableName, SchemaName, ColumnName, DATA_TYPE)
        SELECT 
            TABLE_NAME, 
            TABLE_SCHEMA,
            COLUMN_NAME,
            DATA_TYPE
        FROM ' + QUOTENAME(@DatabaseName) + '.INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            DATA_TYPE = ''int''
            AND TABLE_NAME IN (SELECT TABLE_NAME FROM ' + QUOTENAME(@DatabaseName) + '.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = ''BASE TABLE'')';
        
        EXEC sp_executesql @SQL;

        -- Get the total number of INT columns to process
        SELECT @TotalRows = COUNT(*) FROM #IntColumns;

        -- Temporary table to store results for the current database
        DROP TABLE IF EXISTS #MaxValues;
        CREATE TABLE #MaxValues (
            DatabaseName NVARCHAR(128),
            SchemaName NVARCHAR(128),
            TableName NVARCHAR(128),
            ColumnName NVARCHAR(128),
            Data_Type NVARCHAR(128),  
            MaxValue INT
        );

        -- WHILE loop to process each row in #IntColumns for the current database
        WHILE @RowIndex <= @TotalRows
        BEGIN
            -- Get the current table, schema, column name, and data type based on RowIndex
            SELECT @TableName = TableName, @SchemaName = SchemaName, @ColumnName = ColumnName, @DataType = DATA_TYPE
            FROM #IntColumns
            WHERE Id = @RowIndex;

            -- Build dynamic SQL to get the MAX value for the current column with fully qualified database, schema, and table names
            SET @SQL = 'SELECT @MaxValue_OUT = MAX(' + QUOTENAME(@ColumnName) + ') 
                        FROM ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

            -- Execute the dynamic SQL
            BEGIN TRY
                EXEC sp_executesql @SQL, N'@MaxValue_OUT INT OUTPUT', @MaxValue_OUT = @MaxValue OUTPUT;

                -- Insert the result into the #MaxValues table
                INSERT INTO #MaxValues (DatabaseName, SchemaName, TableName, ColumnName, DATA_TYPE, MaxValue)
                VALUES (@DatabaseName, @SchemaName, @TableName, @ColumnName, @DataType, @MaxValue);
            END TRY
            BEGIN CATCH
                -- Handle any errors gracefully
                PRINT 'Error processing table ' + @SchemaName + '.' + @TableName + ' and column ' + @ColumnName + ' in database ' + @DatabaseName;
            END CATCH;

            -- Increment the RowIndex
            SET @RowIndex = @RowIndex + 1;
        END;

        -- Append results of this database to the final result table
        INSERT INTO #AllMaxValues
        SELECT * FROM #MaxValues;

    END TRY
    BEGIN CATCH
        PRINT 'Error accessing database: ' + @DatabaseName;
    END CATCH;

    -- Move to the next database
    SET @DbIndex = @DbIndex + 1;
END;

-- Select the final results for all databases
SELECT * FROM #AllMaxValues
WHERE MaxValue > 200000000  -- Filter for max values greater than 200,000,000 200000000
ORDER BY MaxValue;

-- Drop the temporary result table
DROP TABLE IF EXISTS #AllMaxValues;
