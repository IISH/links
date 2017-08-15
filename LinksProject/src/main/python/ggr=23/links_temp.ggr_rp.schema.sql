
--
-- Table structure for table `ggr_r`
--

DROP TABLE IF EXISTS `ggr_r`;
CREATE TABLE `ggr_r` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `id_source` int(10) unsigned DEFAULT NULL,
  `name_source` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `id_orig_registration` int(10) unsigned DEFAULT NULL,
  `registration_maintype` tinyint(3) unsigned DEFAULT NULL,
  `registration_type` varchar(50) COLLATE utf8_bin  DEFAULT NULL,
  `registration_day` tinyint(3) unsigned DEFAULT NULL,
  `registration_month` tinyint(3) unsigned DEFAULT NULL,
  `registration_year` smallint(5) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_orig_registration` (`id_orig_registration`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

--
-- Table structure for table `ggr_p`
--

DROP TABLE IF EXISTS `ggr_p`;
CREATE TABLE `ggr_p` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `id_source` int(10) unsigned DEFAULT NULL,
  `name_source` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `id_orig_registration` int(10) unsigned DEFAULT NULL,
  `id_person_o` int(10) unsigned DEFAULT NULL,
  `registration_maintype` tinyint(3) unsigned DEFAULT NULL,
  `registration_type` varchar(50) COLLATE utf8_bin  DEFAULT NULL,
  `sex` char(1) COLLATE utf8_bin DEFAULT NULL,
  `role` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `firstname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `familyname` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `birth_day` tinyint(3) unsigned DEFAULT NULL,
  `birth_month` tinyint(3) unsigned DEFAULT NULL,
  `birth_year` smallint(5) unsigned DEFAULT NULL,
  `birth_date` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
