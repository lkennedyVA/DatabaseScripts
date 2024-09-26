SELECT name AS synonym_name, base_object_name
FROM sys.synonyms
WHERE name LIKE '%Batch%'
order by base_object_name;
