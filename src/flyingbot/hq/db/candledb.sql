CREATE DATABASE candledb
	CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
    
CREATE TABLE `candledb`.`candle_01` (
	`DBID` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'ID of the candle, for fast sorting',
	`ProductID` CHAR(16) COMMENT 'product id, the first few characters of instrument id',
    `InstrumentID` CHAR(32) COMMENT 'instrument id',
    `Period` SMALLINT COMMENT 'Period in minutes, for day it is 1440(mins)',
    `SerialNo` CHAR(32) COMMENT 'Serial number for the candle, noting the intial time',
    `JSON` TEXT COMMENT 'JSON string of the candle',
    KEY(`ProductID`),
    KEY(`InstrumentID`),
    KEY(`Period`),
    PRIMARY KEY(`DBID`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;