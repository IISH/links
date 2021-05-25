-- MySQL dump 10.14  Distrib 5.5.64-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: links_logs
-- ------------------------------------------------------
-- Server version	5.5.64-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `ERROR_STORE`
--

DROP TABLE IF EXISTS `ERROR_STORE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ERROR_STORE` (
  `id_log` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `id_source` int(10) unsigned DEFAULT NULL,
  `archive` varchar(30) DEFAULT NULL,
  `scan_url` varchar(256) DEFAULT NULL,
  `location` varchar(120) DEFAULT NULL,
  `reg_type` varchar(50) DEFAULT NULL,
  `date` varchar(25) DEFAULT NULL,
  `sequence` varchar(60) DEFAULT NULL,
  `role` varchar(30) DEFAULT NULL,
  `guid` varchar(80) DEFAULT NULL,
  `reg_key` int(10) unsigned DEFAULT NULL,
  `pers_key` int(10) unsigned DEFAULT NULL,
  `report_class` varchar(2) DEFAULT NULL,
  `report_type` int(10) unsigned DEFAULT NULL,
  `content` varchar(200) DEFAULT NULL,
  `date_time` datetime NOT NULL,
  `flag` tinyint(3) DEFAULT NULL,
  `date_export` varchar(25) DEFAULT NULL,
  `destination` varchar(30) DEFAULT NULL,
  PRIMARY KEY (`id_log`),
  UNIQUE KEY `unique_index` (`id_source`,`location`,`reg_type`,`date`,`sequence`,`role`,`content`)
) ENGINE=InnoDB AUTO_INCREMENT=7579 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2021-05-19 13:38:22
