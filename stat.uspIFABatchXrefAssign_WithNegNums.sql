ALTER PROCEDURE [stat].[uspIFABatchXrefAssign]
    (
         @piSourceBatchId int -- required
        ,@pvBatchSource varchar(32) -- required

        ,@piIFABatchId int OUTPUT
        ,@pvErrorMessage varchar(255) OUTPUT
    )
AS
BEGIN
    DECLARE
         @iReturn int = 0 -- 0 = Success, !0 = Issue encountered
        ,@iSourceBatchId int = @piSourceBatchId
        ,@vBatchSource varchar(32) = @pvBatchSource
        ,@iIFABatchSeq int = NULL
        ,@iIFABatchId int = NULL
        ,@iIFABatchVersionSeq int = NULL
        ,@iMaxPositiveId int = 32767 -- Max for smallint; adjust based on your actual column type
    ;

    -- Check if SourceBatchId and BatchSource are already in use
    SELECT 
         @iIFABatchId = [IFABatchId]
        ,@iIFABatchVersionSeq = [IFABatchVersionSeq]
    FROM [stat].[IFABatchXref]
    WHERE [SourceBatchId] = @iSourceBatchId
        AND [BatchSource] = @vBatchSource
    ;

    IF ISNULL( @iIFABatchId, 0 ) <> 0
    BEGIN
        -- If SourceBatchId + BatchSource are already in use, return an error
        SELECT
             @iReturn = @iIFABatchId
            ,@pvErrorMessage = ISNULL( ( 'Provided "' + @vBatchSource + '" source batch Id ' + CONVERT( varchar(20), @iSourceBatchId ) + ' is in use.' ), '{none}' )
        ;
        RETURN ( @iReturn )
        ;
    END

    -- Check for available positive BatchId
    SELECT @iIFABatchSeq = MIN( [IFABatchSeq] )
    FROM [stat].[IFABatchXref]
    WHERE [IFABatchId] IS NULL
      AND [IFABatchSeq] > 0 -- Only positive IDs
    ;

    IF @iIFABatchSeq IS NULL
    BEGIN
        -- All positive BatchIDs are taken; now check for negative ones
        SELECT @iIFABatchSeq = MIN( [IFABatchSeq] )
        FROM [stat].[IFABatchXref]
        WHERE [IFABatchId] IS NULL
          AND [IFABatchSeq] < 0 -- Now, look for negative IDs
        ;
        
        IF @iIFABatchSeq IS NULL
        BEGIN
            -- No available BatchIDs left, return error
            SET @pvErrorMessage = 'No available BatchID for assignment.'
            RETURN -1
        END
    END

    -- Determine next IFABatchVersionSeq for the BatchID
    SELECT 
         @iIFABatchVersionSeq = ISNULL(MAX([IFABatchVersionSeq]) + 1, 1)
    FROM [stat].[IFABatchXrefHistory]
    WHERE [IFABatchId] = @iIFABatchSeq
    ;

    BEGIN TRY
        -- Assign the BatchId by updating the row in [stat].[IFABatchXref]
        UPDATE u SET 
             [IFABatchId] = @iIFABatchSeq
            ,[IFABatchVersionSeq] = @iIFABatchVersionSeq
            ,[SourceBatchId] = @iSourceBatchId
            ,[BatchSource] = @vBatchSource
            ,[IFABatchVersionEffectDatetime] = SYSDATETIME()
            ,[IFABatchVersionVerifyDatetime] = NULL
        FROM [stat].[IFABatchXref] AS u
        WHERE u.[IFABatchSeq] = @iIFABatchSeq
        ;

        -- Return the assigned IFABatchId via the output parameter
        SET @piIFABatchId = @iIFABatchSeq

    END TRY
    BEGIN CATCH
        -- Handle error
        SET @iReturn = -1
        SET @pvErrorMessage = ERROR_MESSAGE();
		PRINT '...error handling code goes here...';
        THROW;
    END CATCH

    RETURN ( @iReturn )
END
