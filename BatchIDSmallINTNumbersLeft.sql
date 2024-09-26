select 
	max(statbatchid) as 'Last Batch Id Assigned',
	32788 - max(statbatchid) as 'Batch Ids Remaining'
from [CentralRisk].[dbo].[StatBatchIdentityLog];