DELIMITER $$

CREATE 
	EVENT `archive_candles` 
	ON SCHEDULE EVERY 1 DAY STARTS '2018-12-24 16:00:00' 
	DO BEGIN
		-- Total candle number
		SET @total = 0;
		SELECT COUNT(*) INTO @total FROM candledb.candle_01;
        
        -- Number of candles to save
		SET @toDelete = @total - 10000000;
        
        -- Check if we need to save some candles
        IF @toDelete > 0 THEN
			-- Find the secure-file-priv
			SET @OutDir = '';
			SELECT @@secure_file_priv INTO @OutDir;
			SET @OutDir = REPLACE(@OutDir, '\\', "/");
			
			-- Save candles
			SET @SqlText = CONCAT("SELECT * FROM candledb.candle_01 ORDER BY `DBID` ASC LIMIT ",
				@toDelete,
				" INTO OUTFILE '",
				@OutDir,
				"candledb.", 
				DATE_FORMAT(now(),"%Y%m%d_%H%i%s_%p"), 
				".csv' FIELDS ENCLOSED BY '' TERMINATED BY '\t'  LINES TERMINATED BY '\r\n';");
			
			-- Execute query
			PREPARE SqlStat FROM @SqlText;
			EXECUTE SqlStat;
			
			-- Delete candles from database
			SET @SqlText = CONCAT("DELETE FROM candledb.candle_01 ORDER BY `DBID` ASC LIMIT ", @toDelete);
			
			-- Execute query
			PREPARE SqlStat FROM @SqlText;
			EXECUTE SqlStat;
		END IF;
	END $$

DELIMITER ;