-- MySQL dump 10.14  Distrib 5.5.60-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: links_temp
-- ------------------------------------------------------
-- Server version	5.5.60-MariaDB

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
-- Table structure for table `militieregisters11`
--

DROP TABLE IF EXISTS `militieregisters11`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `militieregisters11` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `Id_orig_registration` int(10) unsigned DEFAULT NULL,
  `Id_person_o` int(10) unsigned DEFAULT NULL,
  `Id_source` int(10) unsigned DEFAULT NULL,
  `name_source` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Registration_maintype` tinyint(3) unsigned DEFAULT NULL,
  `Registration_type` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `Registration_day` tinyint(3) DEFAULT NULL,
  `Registration_month` tinyint(3) DEFAULT NULL,
  `Registration_year` smallint(3) DEFAULT NULL,
  `Role` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `Idnr` int(10) unsigned DEFAULT NULL,
  `Persnr` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Relatie_type` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Achternaam` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Prefix` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Voornaam` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Geslacht` char(1) COLLATE utf8_bin DEFAULT NULL,
  `Gebdag` tinyint(3) DEFAULT NULL,
  `Gebmnd` tinyint(3) DEFAULT NULL,
  `Gebjaar` smallint(3) DEFAULT NULL,
  `Gebplaats` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `temp` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=196919 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-08-28 10:37:06
