-- ============================================================== --
-- Get All databases size                                         --
-- ==============================                                 --
-- Based on sp_spaceused                                          --
--                                                                --
-- Notes                                                          --
-- =====                                                          --
-- Have fun!                                                      --
--                                                                --
-- ============================================================== --
USE [master]
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
SET DEADLOCK_PRIORITY LOW;

DECLARE
 @BigCommand NVARCHAR(2000) = N''
,@UpdateUsage NVARCHAR(7) = '''false''' -- true/false

-- Drop temporary tables
IF OBJECT_ID('tempdb..#DBSizeInfo') IS NOT NULL DROP TABLE #DBSizeInfo

-- Get detailed size information for all databases on the instance
CREATE TABLE #DBSizeInfo (
   [DBName]       sysname
  ,[TotalSize]    DECIMAL(15,2)
  ,[Unallocated]  DECIMAL(15,2)
  ,[Reserved]     DECIMAL(15,2)
  ,[Data]         DECIMAL(15,2)
  ,[Index]        DECIMAL(15,2)
  ,[Unused]       DECIMAL(15,2)
);

SET @BigCommand = N'IF ''?'' IN (''master'',''model'',''msdb'',''tempdb'') RETURN;'
+ 'Use [?];'
+ 'DECLARE @updateusage VARCHAR(5)=' + @UpdateUsage
+ ',@PageSizeInBytes SMALLINT = 8192'
+ ',@MBInBytes INTEGER = 1048576'
+ ',@GBInBytes BIGINT = 1073741824'
+ NCHAR(13) + NCHAR(10)
+ ',@id INT'                                  -- The object id that takes up space
+ ',@type CHARACTER(2)'                       -- The object type.
+ ',@pages BIGINT'                            -- Working variable for size calc.
+ ',@dbname sysname'
+ ',@dbsize BIGINT'
+ ',@logsize BIGINT'
+ ',@reservedpages BIGINT'
+ ',@usedpages BIGINT'
+ ',@rowCount BIGINT'
+ NCHAR(13) + NCHAR(10)
+ ',@TotalSize DECIMAL(15,2)'
+ ',@UnallocatedSpace DECIMAL(15,2)'
+ ',@Reserved DECIMAL(15,2)'
+ ',@DataSize DECIMAL(15,2)'
+ ',@IndexSize DECIMAL(15,2)'
+ ',@UnusedSpace DECIMAL(15,2);'
+ NCHAR(13) + NCHAR(10)

-- Check to see if user wants usages updated.
+ 'IF @updateusage IS NOT NULL '
+ 'BEGIN '
+ 'SELECT  @updateusage=LOWER(@updateusage);'
+ 'IF @updateusage NOT IN (''true'',''false'')'
+ 'BEGIN '
+ 'RAISERROR(15143,-1,-1,@updateusage);'
+ 'END;'
+ 'END;'
+ NCHAR(13) + NCHAR(10)
-- Update usages if user specified to do so.
+ 'IF @updateusage=''true'''
+ 'BEGIN '
+ 'DBCC UPDATEUSAGE(0) WITH NO_INFOMSGS;'
+ 'PRINT '' '';'
+ 'END;'
+ NCHAR(13) + NCHAR(10)
-- Get Database and log size from sys.sysfiles
+ 'SELECT @dbsize=SUM(CONVERT(BIGINT,CASE WHEN status&64=0 THEN size ELSE 0 END))'
+ ',@logsize = SUM(CONVERT(BIGINT,CASE WHEN status&64<>0 THEN size ELSE 0 END)) '
+ 'FROM dbo.sysfiles;'
+ NCHAR(13) + NCHAR(10)
-- Calculate total size based on files
+ 'SET @TotalSize=(CONVERT(DEC(15,2),@dbsize)+CONVERT(DEC(15,2),@logsize))*@PageSizeInBytes/@MBInBytes;'
+ NCHAR(13) + NCHAR(10)
-- Calculate size based on pages
+ 'SELECT @reservedpages=SUM(a.total_pages)'
+ ',@usedpages=SUM(a.used_pages)'
+ ',@pages = SUM(CASE WHEN it.internal_type IN (202,204,207,211,212,213,214,215,216,221,222,236) THEN 0 '
+ 'WHEN a.type<>1 AND p.index_id<2 THEN a.used_pages '
+ 'WHEN p.index_id<2 THEN a.data_pages ELSE 0 END) '
+ 'FROM sys.partitions p JOIN sys.allocation_units a ON p.partition_id=a.container_id '
+ 'LEFT JOIN sys.internal_tables it ON p.object_id = it.object_id;'
+ NCHAR(13) + NCHAR(10)
-- Unallocated space could not be negative
+ 'SET @UnallocatedSpace=0;'
+ 'IF @dbsize>=@reservedpages '
+ 'BEGIN SET @UnallocatedSpace=(CONVERT(DEC(15,2),@dbsize)-CONVERT(DEC(15,2),@reservedpages))*@PageSizeInBytes/@MBInBytes; END;'
+ NCHAR(13) + NCHAR(10)
+ 'SET @Reserved=@reservedpages*@PageSizeInBytes/@MBInBytes;'                     -- reserved: sum(reserved) where indid in (0, 1, 255)
+ 'SET @DataSize=@pages*@PageSizeInBytes/@MBInBytes;'                             -- data: sum(data_pages) + sum(text_used)
+ 'SET @IndexSize=(@usedpages-@pages)*@PageSizeInBytes/@MBInBytes;'               -- index: sum(used) where indid in (0, 1, 255) - data
+ 'SET @UnusedSpace=(@reservedpages-@usedpages)*@PageSizeInBytes/@MBInBytes;'     -- unused: sum(reserved) - sum(used) where indid in (0, 1, 255)
+ NCHAR(13) + NCHAR(10)
+ 'INSERT INTO #DBSizeInfo VALUES(''?'',@TotalSize,@UnallocatedSpace,@Reserved,@DataSize,@IndexSize,@UnusedSpace)';

-- Uncomment to debug
--PRINT LEN(@BigCommand);
--PRINT @BigCommand;

-- Iterate through all non-system databases, retrieving space/size info from each, populating a temporary table
EXEC sp_MSforeachdb @BigCommand;

-- Display the results
SELECT
  DBName AS [Database_Name]
  ,CASE WHEN TotalSize > CAST(1000 AS DECIMAL(15,2)) THEN LTRIM(STR(TotalSize/1024,15,2)) + ' GB' ELSE LTRIM(STR(TotalSize,15,2)) + ' MB' END AS [Total_Size]
  ,CASE WHEN Unallocated > CAST(1000 AS DECIMAL(15,2)) THEN LTRIM(STR(Unallocated/1024,15,2)) + ' GB' ELSE LTRIM(STR(Unallocated,15,2)) + ' MB' END AS [Unallocated_Space]
  ,CASE WHEN Reserved > CAST(1000 AS DECIMAL(15,2)) THEN LTRIM(STR(Reserved/1024,15,2)) + ' GB' ELSE LTRIM(STR(Reserved,15,2)) + ' MB' END AS [Reserved_Space]
  ,CASE WHEN Data > CAST(1000 AS DECIMAL(15,2)) THEN LTRIM(STR(Data/1024,15,2)) + ' GB' ELSE LTRIM(STR(Data,15,2)) + ' MB' END AS [Data_Size]
  ,CASE WHEN [Index] > CAST(1000 AS DECIMAL(15,2)) THEN LTRIM(STR([Index]/1024,15,2)) + ' GB' ELSE LTRIM(STR([Index],15,2)) + ' MB' END AS [Index_Size]
  ,CASE WHEN Unused > CAST(1000 AS DECIMAL(15,2)) THEN LTRIM(STR(Unused/1024,15,2)) + ' GB' ELSE LTRIM(STR(Unused,15,2)) + ' MB' END AS [Unused_Space]
  ,FORMAT(ROUND(Unallocated/TotalSize,2),'p') AS [PCT_Free]
FROM #DBSizeInfo ORDER BY TotalSize DESC;

-- Drop temporary tables
IF OBJECT_ID('tempdb..#DBSizeInfo') IS NOT NULL DROP TABLE #DBSizeInfo;

GO