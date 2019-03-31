/*
 Navicat MySQL Data Transfer

 Source Server         : 公众号
 Source Server Type    : MySQL
 Source Server Version : 50562
 Source Host           : 123.57.81.203
 Source Database       : limit_up

 Target Server Type    : MySQL
 Target Server Version : 50562
 File Encoding         : utf-8

 Date: 03/31/2019 15:06:51 PM
*/

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `tick_daily`
-- ----------------------------
DROP TABLE IF EXISTS `tick_daily`;
CREATE TABLE `tick_daily` (
  `code` varchar(20) COLLATE utf8_general_mysql500_ci NOT NULL,
  `close` decimal(10,2) DEFAULT NULL,
  `trade_date` date NOT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `name` varchar(255) COLLATE utf8_general_mysql500_ci DEFAULT NULL,
  `now` double DEFAULT NULL,
  `open` double DEFAULT NULL,
  `trade_time` varchar(20) COLLATE utf8_general_mysql500_ci NOT NULL,
  `turnover` bigint(20) DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `chg` decimal(10,4) DEFAULT NULL,
  `query_time` time NOT NULL,
  `bid1` double DEFAULT NULL,
  PRIMARY KEY (`query_time`,`trade_time`,`trade_date`,`code`),
  UNIQUE KEY `unique_index` (`code`,`trade_date`,`trade_time`,`query_time`) USING BTREE,
  KEY `date_time` (`trade_date`,`trade_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_mysql500_ci;

SET FOREIGN_KEY_CHECKS = 1;
