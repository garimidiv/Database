SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Admin].[pRunArchivePurgeAll_byDate]') AND type in (N'P', N'PC'))
DROP PROCEDURE [Admin].[pRunArchivePurgeAll_byDate]
GO

Create PROCEDURE [Admin].[pRunArchivePurgeAll_byDate] (
	@ArchiveGroup INT=1,
	@PnLDate Date
)
AS
	BEGIN 

	SET NOCOUNT ON
	--------------------------------------------------------------------------------------------------------------------------
	-- Get list of tables to be Purged/Archived for the day
	--------------------------------------------------------------------------------------------------------------------------
	SELECT 
		SequenceId
		,ArchiveType
		,LiveSchemaName
		,LiveTableName
		,ArchiveSchemaName
		,ArchiveTableName
		,FilterColumnName
		,MoveToArchive
		,LiveAdditionalFilter
		,LiveJoin
		,ArchiveAdditionalFilter
		,ArchiveJoin
		,LiveRetentionDays
		,ArchiveRetentionDays
		,BatchSize
		,LiveNumKeepMonthends
		,ArchiveNumKeepMonthends
	INTO #TABLESTOARCHIVETODAY
	FROM Admin.tTableArchiveConfigurations With(Nolock) WHERE doArchive='Y' and DaysToRun IN ('d',CAST(DATENAME(dw,GETDATE()) AS VARCHAR(3))) AND ArchiveGroup =@ArchiveGroup ORDER BY SequenceId ASC

	--------------------------------------------------------------------------------------------------------------------------
	-- Variables
	--------------------------------------------------------------------------------------------------------------------------
	DECLARE @SequenceId INT
	DECLARE @LiveSchemaName VARCHAR(100)
	DECLARE @LiveTableName VARCHAR(200)
	DECLARE @ArchiveSchemaName VARCHAR(100)
	DECLARE @ArchiveTableName VARCHAR(200)
	DECLARE @FilterColumnName VARCHAR(100)
	DECLARE @LiveAdditionalFilter VARCHAR(2000)
	DECLARE @LiveJoin VARCHAR(2000)
	DECLARE @ArchiveAdditionalFilter VARCHAR(2000)
	DECLARE @ArchiveJoin VARCHAR(2000)
	DECLARE @LiveRetentionDays INT
	DECLARE @ArchiveRetentionDays INT
	DECLARE @BatchSize INT
	DECLARE @MoveToArchive CHAR(1)
	DECLARE @LiveNumKeepMonthends INT
	DECLARE @ArchiveNumKeepMonthends INT
	DECLARE @LiveCutoffDate DATE
	DECLARE @ArchiveCutoffDate DATE
	DECLARE @LiveTable VARCHAR(200)
	DECLARE @ArchiveTable VARCHAR(200)
	DECLARE @AdditionalLiveDateFilter VARCHAR(2000)
	DECLARE @AdditionalArchiveDateFilter VARCHAR(2000)
	DECLARE @RC INT
	DECLARE @LiveCountBefore BIGINT
	DECLARE @LiveCountAfter BIGINT
	DECLARE @ArchiveCountBefore BIGINT
	DECLARE @ArchiveCountAfter BIGINT
	DECLARE @StartTime DATETIME
	DECLARE @EndTime DATETIME
	DECLARE @t_SQL NVARCHAR(500)
	DECLARE @t_parameters NVARCHAR(100)
	DECLARE @i_Count BIGINT
	DECLARE @ErrorMessage VARCHAR(2000)
	DECLARE @MovedToArchiveCount BIGINT
	DECLARE @PurgedFromArchiveCount BIGINT
	DECLARE @ArchiveType VARCHAR(50)

	--------------------------------------------------------------------------------------------------------------------------
	-- Run Archive/Purge for Each table from List above
	--------------------------------------------------------------------------------------------------------------------------
	WHILE 1=1 
	BEGIN 
		SET @ErrorMessage = 'NA'

		SET @AdditionalLiveDateFilter = NULL
		SET @AdditionalArchiveDateFilter = NULL

		SELECT TOP 1 
			@SequenceId = SequenceId,
			@ArchiveType = ArchiveType,
			@LiveSchemaName=LiveSchemaName,
			@LiveTableName=LiveTableName,
			@ArchiveSchemaName=ArchiveSchemaName,
			@ArchiveTableName=ArchiveTableName,
			@FilterColumnName=FilterColumnName,
			@LiveAdditionalFilter=LiveAdditionalFilter,
			@LiveJoin=LiveJoin,
			@ArchiveAdditionalFilter=ArchiveAdditionalFilter,
			@ArchiveJoin=ArchiveJoin,
			@LiveRetentionDays=LiveRetentionDays,
			@ArchiveRetentionDays=ArchiveRetentionDays,
			@BatchSize=BatchSize,
			@MoveToArchive=MoveToArchive,
			@LiveNumKeepMonthends=LiveNumKeepMonthends,
			@ArchiveNumKeepMonthends=ArchiveNumKeepMonthends
		FROM #TABLESTOARCHIVETODAY 
		ORDER BY SequenceId ASC

		IF NOT EXISTS (SELECT 1 FROM #TABLESTOARCHIVETODAY)
			BREAK;

		--Calculate Cutoff Dates
		DECLARE @SQLString NVARCHAR(500)
		DECLARE @parameters NVARCHAR(100)
		DECLARE @MaxDate DATE
		SET @parameters = '@MaxDate DATE OUTPUT'
		
			
		IF @ArchiveType = 'BYDATE'
		BEGIN
			SET @SQLString = N'SELECT @MaxDate = MAX('+ @FilterColumnName + ') FROM ' + @LiveSchemaName +'.'+ @LiveTableName;
			EXEC sp_executesql @SQLString, @parameters, @MaxDate = @MaxDate OUTPUT

			set @MaxDate = @PnLDate

			--Prepare to call ArchivePurge
			SELECT @LiveCutoffDate = [Global].[GetNthPreviousWorkingDay](@MaxDate, - @LiveRetentionDays)

		END
		IF @ArchiveType = 'SNAPSHOT' 
		BEGIN
			SET @SQLString = N'SELECT @MaxDate = MAX('+ @FilterColumnName + ') FROM ' + @ArchiveSchemaName +'.'+ @ArchiveTableName;
			EXEC sp_executesql @SQLString, @parameters, @MaxDate = @MaxDate OUTPUT

			set @MaxDate = @PnLDate

			--Prepare to call ArchivePurge
			SELECT @LiveCutoffDate = [Global].[GetNthPreviousWorkingDay](@MaxDate, - @ArchiveRetentionDays)

		END

		IF @MoveToArchive ='Y' AND @ArchiveType = 'BYDATE'
			SELECT @ArchiveCutoffDate = [Global].[GetNthPreviousWorkingDay](@LiveCutoffDate, - @ArchiveRetentionDays)
		ELSE IF @ArchiveType = 'SNAPSHOT' 
			SELECT @ArchiveCutoffDate = @LiveCutoffDate
		ELSE
			SET @ArchiveCutoffDate = NULL

		--Set @ArchiveCutoffDate = @PnLDate

		IF @LiveNumKeepMonthends >0
			SET @AdditionalLiveDateFilter = ' AND T.' + @FilterColumnName +' NOT IN (' + Admin.fnGetPrevMonthEndDates (CAST(GETDATE() AS DATE),@LiveNumKeepMonthends) + ')'

		IF @ArchiveNumKeepMonthends >0
			SET @AdditionalArchiveDateFilter = ' AND T.' + @FilterColumnName +' NOT IN (' + Admin.fnGetPrevMonthEndDates (CAST(@LiveCutoffDate AS DATE),@ArchiveNumKeepMonthends) + ')'

		-----------------------------------------------------------------------------------------------------------------------
		-- Collect Before Stats
		-----------------------------------------------------------------------------------------------------------------------
		SET @t_parameters = '@i_Count BIGINT OUTPUT'
		SET @t_SQL = N'SELECT @i_Count = Sum(p.rows)
					FROM sys.partitions AS p
					INNER JOIN sys.tables AS t ON p.[object_id] = t.[object_id]
					INNER JOIN sys.schemas AS s ON s.[schema_id] = t.[schema_id]
					INNER JOIN sys.indexes IDX ON P.object_id = IDX.object_id
					AND P.index_id = IDX.index_id 
					WHERE S.name = N'+ ''''+ @LiveSchemaName +''''+ +' AND t.name = N'+''''+ @LiveTableName +''''
					+' AND p.index_id IN (0,1)';
		EXEC sp_executesql @t_SQL, @t_parameters, @i_Count = @i_Count OUTPUT
		SELECt @LiveCountBefore=@i_Count

		IF @MoveToArchive ='Y'
		BEGIN
			SET @t_SQL = N'SELECT @i_Count = Sum(p.rows)
					FROM sys.partitions AS p
					INNER JOIN sys.tables AS t ON p.[object_id] = t.[object_id]
					INNER JOIN sys.schemas AS s ON s.[schema_id] = t.[schema_id]
					INNER JOIN sys.indexes IDX ON P.object_id = IDX.object_id
					AND P.index_id = IDX.index_id 
					WHERE S.name = N'+ ''''+ @ArchiveSchemaName +''''+ +' AND t.name = N'+''''+ @ArchiveTableName +''''
					+' AND p.index_id IN (0,1)';
			EXEC sp_executesql @t_SQL, @t_parameters, @i_Count = @i_Count OUTPUT
			SELECT @ArchiveCountBefore=@i_Count
		END
		ELSE
		BEGIN
			SELECT @ArchiveCountBefore=0
		END

		SELECT @StartTime=GETDATE()

		-----------------------------------------------------------------------------------------------------------------------
		-- Run Archiving/Purging Proc for Table
		-----------------------------------------------------------------------------------------------------------------------
		BEGIN TRY
			
			IF @ArchiveType = 'BYDATE'
			BEGIN
				EXEC [Admin].[pArchivePurgeTableByDate]
					@LiveSchemaName, 
					@LiveTableName,
					@FilterColumnName, 
					@ArchiveSchemaName,
					@ArchiveTableName,
					@MoveToArchive,
					@LiveAdditionalFilter,
					@ArchiveAdditionalFilter,
					@BatchSize,
					NULL,
					NULL,
					@LiveCutoffDate,
					@ArchiveCutoffDate,
					@AdditionalLiveDateFilter,
					@AdditionalArchiveDateFilter
			END

			IF @ArchiveType = 'SNAPSHOT'
			BEGIN
				EXEC [Admin].[pArchiveSnapshots]
					@LiveSchemaName, 
					@LiveTableName,
					@FilterColumnName, 
					@ArchiveSchemaName,
					@ArchiveTableName,
					@MoveToArchive,
					@LiveAdditionalFilter,
					@ArchiveAdditionalFilter,
					@BatchSize,
					NULL,
					NULL,
					@LiveCutoffDate,
					@ArchiveCutoffDate,
					@AdditionalLiveDateFilter,
					@AdditionalArchiveDateFilter

			END			

		END TRY

		BEGIN CATCH
			SET @ErrorMessage= error_message()
		END CATCH

		-----------------------------------------------------------------------------------------------------------------------
		-- Collect After Stats
		-----------------------------------------------------------------------------------------------------------------------

		SET @t_parameters = '@i_Count BIGINT OUTPUT'
		SET @t_SQL = N'SELECT @i_Count = Sum(p.rows)
					FROM sys.partitions AS p
					INNER JOIN sys.tables AS t ON p.[object_id] = t.[object_id]
					INNER JOIN sys.schemas AS s ON s.[schema_id] = t.[schema_id]
					INNER JOIN sys.indexes IDX ON P.object_id = IDX.object_id
					AND P.index_id = IDX.index_id 
					WHERE S.name = N'+ ''''+ @LiveSchemaName +''''+ +' AND t.name = N'+''''+ @LiveTableName +''''
					+' AND p.index_id IN (0,1)';
		EXEC sp_executesql @t_SQL, @t_parameters, @i_Count = @i_Count OUTPUT
		SELECt @LiveCountAfter=@i_Count

		IF @MoveToArchive ='Y'
		BEGIN
			SET @t_SQL =N'SELECT @i_Count = Sum(p.rows)
					FROM sys.partitions AS p
					INNER JOIN sys.tables AS t ON p.[object_id] = t.[object_id]
					INNER JOIN sys.schemas AS s ON s.[schema_id] = t.[schema_id]
					INNER JOIN sys.indexes IDX ON P.object_id = IDX.object_id
					AND P.index_id = IDX.index_id 
					WHERE S.name = N'+ ''''+ @ArchiveSchemaName +''''+ +' AND t.name = N'+''''+ @ArchiveTableName +''''
					+' AND p.index_id IN (0,1)';
			EXEC sp_executesql @t_SQL, @t_parameters, @i_Count = @i_Count OUTPUT
			SELECT @ArchiveCountAfter=@i_Count
		END
		ELSE
		BEGIN
			SELECT @ArchiveCountAfter=0
		END

		SELECT @EndTime=GETDATE()
		-----------------------------------------------------------------------------------------------------------------------
		-- Log Archive/Purge Stats
		-----------------------------------------------------------------------------------------------------------------------

		IF @MoveToArchive ='Y'
			SET @MovedToArchiveCount = (@LiveCountBefore- @LiveCountAfter)
		ELSE
			SET @MovedToArchiveCount = 0

		IF @MoveToArchive ='Y'
			SET @PurgedFromArchiveCount = ((@ArchiveCountBefore + ((@LiveCountBefore- @LiveCountAfter))) - @ArchiveCountAfter)
		ELSE
			SET @PurgedFromArchiveCount = (@LiveCountBefore- @LiveCountAfter)

		DELETE FROM Admin.tArchiveJobLog WHERE RunDate = CAST(GETDATE() AS DATE) AND LiveTableSchema = @LiveSchemaName AND LiveTableName = @LiveTableName

		INSERT INTO Admin.tArchiveJobLog VALUES
		(
			CAST(GETDATE() AS DATE),
			@LiveSchemaName,
			@LiveTableName,
			@LiveCutoffDate,
			@ArchiveCutoffDate,
			@MovedToArchiveCount, 
			@PurgedFromArchiveCount,
			@StartTime,
			@EndTime,
			@ErrorMessage
		)

		----- Updateing ErrorMessages into Admin.tArchiveJobLog tables------
		update Admin.tArchiveJobLog
		set ErrorMessage=TAT.ErrorMessage
		FROM Admin.tArchiveJobLog TA Join [dbo].[tArchiveJobLog_Temp] TAT
		ON TA.RunDate=TAT.RunDate AND TA.LiveTableSchema=TAT.LiveTableSchema AND TA.LiveTableName=TAT.LiveTableName
		Where Isnull(TAT.ErrorMessage,'')<>''

		TRUNCATE TABLE [dbo].[tArchiveJobLog_Temp]
		--Delete processed row
		DELETE FROM #TABLESTOARCHIVETODAY WHERE SequenceId=@SequenceId
		SET @RC=@@ROWCOUNT
		IF @RC=0
			BREAK;

	END --END WHILE

	DROP TABLE #TABLESTOARCHIVETODAY	
END

GO
SET ANSI_PADDING OFF
GO