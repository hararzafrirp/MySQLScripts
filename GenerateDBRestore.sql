-- ============================================================== --
-- Generate Data Restore Commands                                 --
-- ==============================                                 --
-- This script generates data resore commands for databases,      --
-- possibly via NETBIOS connectivity (e.g. \\10.10.10.10\G$).     --
--                                                                --
-- Notes                                                          --
-- =====                                                          --
-- Set @DatabaseName accordingly                                  --
--                                                                --
-- Adapt @BackupsSourceMachine, make sure you have connectivity   --
-- to it from the target machine                                  --
--                                                                --
-- ============================================================== --

SET DEADLOCK_PRIORITY LOW;
SET NOCOUNT ON;

-- Set in order to access the backup files remotely, otherwise set to blank
DECLARE @BackupsSourceMachine NVARCHAR(200) = '\\192.168.16.15\';
--DECLARE @BackupsSourceMachine NVARCHAR(200) = '';

-- Use the required database name. Wildcards are acceptable - %
DECLARE @DatabaseName NVARCHAR(50) = 'HararTest';

-- First couple of drive letters for file relocation
-- Leave empty unless you wish to overwrite!
DECLARE @OverwriteDriveName1 NCHAR(1) = '';
DECLARE @OverwriteDriveName1With NCHAR(1) = '';

-- 2nd couple of drive letters for file relocation
-- Leave empty unless you wish to overwrite!
DECLARE @OverwriteDriveName2 NCHAR(1) = '';
DECLARE @OverwriteDriveName2With NCHAR(1) = '';

DECLARE @PhysicalDeviceName NVARCHAR(2000) = '';

-- Drop temporary tables
IF OBJECT_ID('tempdb..#tempDbsToRestore') IS NOT NULL
    DROP TABLE #tempDbsToRestore
IF OBJECT_ID('tempdb..#tempOrigFiles') IS NOT NULL
    DROP TABLE #tempOrigFiles

CREATE TABLE #tempOrigFiles
    (
     database_name sysname
    ,logical_name sysname
    ,physical_name NVARCHAR(2000)
    );

-- Find the original primary file paths from sys.database_files (as sys.master_files has the replica paths and not the primary paths)
EXEC master..sp_foreachdb @command = '
INSERT INTO #tempOrigFiles
SELECT ''?'', logical_name = df.name, df.physical_name
FROM [?].sys.database_files AS df WITH (NOLOCK)
', @suppress_quotename = 1, @user_only = 1;

WITH    lbs
          AS ( SELECT   bs.database_name
                       ,last_backup_set_id = MAX(bs.backup_set_id)
               FROM     msdb..backupset AS bs
               WHERE    bs.type = 'D'
               GROUP BY bs.database_name
             )
    SELECT identity(int,1,1) AS [RN]
           ,0 AS [MR]
	       ,bs.database_name
           ,bmf.physical_device_name
           ,ofi.logical_name
           ,ofi.physical_name
    INTO    #tempDbsToRestore
    FROM    lbs
    INNER JOIN msdb..backupset AS bs
            ON bs.backup_set_id = lbs.last_backup_set_id
    INNER JOIN msdb..backupfile AS bf
            ON bf.backup_set_id = lbs.last_backup_set_id
    INNER JOIN msdb..backupmediafamily AS bmf
            ON bmf.media_set_id = bs.media_set_id
    INNER JOIN #tempOrigFiles AS ofi
            ON ofi.database_name = bs.database_name
               AND ofi.logical_name = bf.logical_name
    WHERE   bf.state_desc = 'ONLINE'
            AND bs.database_name LIKE @DatabaseName
    ORDER BY lbs.database_name
           ,bf.logical_name;

-- if using a remote location for the backups, replace the backup files full paths
IF @BackupsSourceMachine <> ''
    BEGIN
        UPDATE  #tempDbsToRestore
        SET     physical_device_name = @BackupsSourceMachine + REPLACE(physical_device_name, ':', '$');
    END;

-- Squeezing all backup sets onto a single 'DISK=foo, DISK=bar' string
UPDATE  #tempDbsToRestore
SET     MR = 1
WHERE   RN IN ( SELECT  MIN(t.RN)
                FROM    #tempDbsToRestore AS t
                GROUP BY t.database_name
                       ,t.logical_name );

SELECT @PhysicalDeviceName = RIGHT(t3.physical_device_name_concat,LEN(t3.physical_device_name_concat)-2)
FROM #tempDbsToRestore t
JOIN
(SELECT
t1.database_name
,t1.logical_name
,physical_device_name_concat = STUFF((
  SELECT '''' + NCHAR(10) + ',DISK = ''' + physical_device_name
  FROM #tempDbsToRestore AS t2
  WHERE t1.database_name = t2.database_name
  AND t1.logical_name = t2. logical_name
  FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') + ''''
FROM #tempDbsToRestore t1) AS t3
ON t3.database_name = t.database_name AND t3.logical_name = t.logical_name
;

-- If we wish to relocate files - 1st relocation
IF @OverwriteDriveName1 <> ''
    BEGIN
        UPDATE  #tempDbsToRestore
        SET     physical_name = REPLACE(physical_name, @OverwriteDriveName1+':', @OverwriteDriveName1With+':');
    END;

-- If we wish to relocate files - 1st relocation
IF @OverwriteDriveName2 <> ''
    BEGIN
        UPDATE  #tempDbsToRestore
        SET     physical_name =REPLACE(physical_name, @OverwriteDriveName2+':', @OverwriteDriveName2With+':');
    END;

-- Generate restore commands
SELECT  DISTINCT
        'RESTORE DATABASE [' + t.database_name + ']' + NCHAR(10) + 'FROM ' + @PhysicalDeviceName + NCHAR(10) + ' WITH NORECOVERY'
        + ( SELECT  CONVERT(VARCHAR(MAX), NCHAR(10) + ',MOVE ''' + t_in.logical_name + ''' TO ''' + t_in.physical_name + '''')
            FROM    #tempDbsToRestore AS t_in
            WHERE   t_in.database_name = t.database_name
            ORDER BY t_in.logical_name
          FOR
            XML PATH('')
          ) + ';'
FROM    #tempDbsToRestore AS t
ORDER BY 1;

-- Drop temporary tables
IF OBJECT_ID('tempdb..#tempDbsToRestore') IS NOT NULL
    DROP TABLE #tempDbsToRestore
IF OBJECT_ID('tempdb..#tempOrigFiles') IS NOT NULL
    DROP TABLE #tempOrigFiles
