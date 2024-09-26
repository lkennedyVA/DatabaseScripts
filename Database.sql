SELECT name
FROM sys.databases
WHERE name LIKE '%Atomic%'
   OR name LIKE '%risk%'
   OR name LIKE '%DHayes%'
   order by name