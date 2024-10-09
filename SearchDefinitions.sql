SELECT 
    o.name AS ObjectName,
    o.type_desc AS ObjectType,
    s.name AS SchemaName,
    m.definition AS ObjectDefinition
FROM 
    sys.sql_modules m
INNER JOIN 
    sys.objects o ON m.object_id = o.object_id
INNER JOIN 
    sys.schemas s ON o.schema_id = s.schema_id
WHERE 
    m.definition LIKE '%PrdTrx01%';

----dba.dbo.uspDBDriveSpace
----ifa.common.uspBillingVerificationReport
----ifa.common.uspItemActivity
----ifa.common.uspVaeActivity
----ifa.common.uspVaeQueueActivity
----ifa.ifa.uspDeluxeAdoptionAlert
----ifa.ifa.uspIFATransactionTimingAlerts
----ifa.ifa.uspMainIFAHourlyReport
ifa.ifa.uspProcessRequestDeux
ifa.ifa.uspProcessRequestLarry
----ifa.import.uspOrganizationUpsert
----ifa.import.uspOrganizationValidateInsertOut
----ifa.import.uspValidateBusinessRulesInsertOut
----ifa.import.uspValidateMetaDataInsertOut

SELECT * 
FROM [PrdTrx01].[IFA].[command].[Comment];

