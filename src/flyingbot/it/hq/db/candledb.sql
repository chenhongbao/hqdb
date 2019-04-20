CREATE DATABASE candledb
	CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
    
CREATE TABLE `candle_tmp` (
  `ProductID` char(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'product id, the first few characters of instrument id',
  `InstrumentID` char(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'instrument id',
  `Period` smallint(6) DEFAULT NULL COMMENT 'Period in minutes, for day it is 1440(mins)',
  `SerialNo` char(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Serial number for the candle, noting the intial time',
  `JSON` text COLLATE utf8mb4_unicode_ci COMMENT 'JSON string of the candle',
  KEY `idx_InstrumentID` (`InstrumentID`),
  KEY `idx_Period` (`Period`),
  KEY `idx_SerialNo` (`SerialNo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;