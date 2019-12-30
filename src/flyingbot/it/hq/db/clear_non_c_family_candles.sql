DELIMITER $$
CREATE DEFINER=`logger`@`%` PROCEDURE `clear_non_c_family_candles`(batch_num INT, max_num INT)
BEGIN	
		DECLARE deleted_num, total_num INT DEFAULT 0;
        
        DROP TABLE IF EXISTS running_info;
        
        CREATE TEMPORARY TABLE running_info(
			act_time TIMESTAMP,
            delete_num INT,
            total_num INT
		);
        
        SET deleted_num = 1;
		
		WHILE total_num < max_num AND deleted_num > 0 DO
			# delete
        	DELETE
				FROM candledb.candle_01 
				WHERE ProductID != 'c' AND ProductID != 'cs' 
				LIMIT batch_num;
			
            SET deleted_num = ROW_COUNT();
            
            # update counter
            SET total_num = total_num + deleted_num;
                
			# save running info
            DELETE FROM running_info;
            
            INSERT INTO running_info 
				VALUES(
					CURRENT_TIMESTAMP(),
					deleted_num,
                    total_num);
                    
			# print info
            SELECT * FROM running_info;
            
		END WHILE;
        
		# print info
		SELECT * FROM running_info;
		
		DROP TABLE running_info;
	END
    DELIMITER ;