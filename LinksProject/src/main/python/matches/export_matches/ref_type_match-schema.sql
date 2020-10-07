
--
-- Table structure for table `matches_AB_merged`
--


DROP TABLE IF EXISTS `matches_AB_merged`;
CREATE TABLE `matches_AB_merged` (
  `Id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `GUID_1` varchar(80) DEFAULT NULL,
  `GUID_2` varchar(80) DEFAULT NULL,
  `Type_Match` int(10) DEFAULT NULL,
  `Type_link` int(10) DEFAULT NULL,
  `Quality_link_A` int(10) DEFAULT NULL,
  `Quality_link_B` int(10) DEFAULT NULL,
  `Worth_link int(10) DEFAULT NULL,`
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
