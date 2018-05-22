
DROP TABLE IF EXISTS `militieregisters11`;

CREATE TABLE `militieregisters11` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `Blijft` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `Id_orig_registration` int(10) unsigned DEFAULT NULL,
  `Id_person_o` int(10) unsigned DEFAULT NULL,
  `Id_source` int(10) unsigned DEFAULT NULL,
  `Registration_maintype` tinyint(3) unsigned DEFAULT NULL,
  `Registration_type` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `Registration_day` tinyint(3) unsigned DEFAULT NULL,
  `Registration_month` tinyint(3) unsigned DEFAULT NULL,
  `Registration_year` smallint(3) unsigned DEFAULT NULL,
  `Role` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `Idnr` int(10) unsigned DEFAULT NULL,
  `Persnr` int(10) unsigned DEFAULT NULL,
  `Relatie_type` int(10) unsigned DEFAULT NULL,
  `Achternaam_prefix` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Achternaam` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Prefix` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Voornaam` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `Geslacht` char(1) COLLATE utf8_bin DEFAULT NULL,
  `Gebdag` tinyint(3) unsigned DEFAULT NULL,
  `Gebmnd` tinyint(3) unsigned DEFAULT NULL,
  `Gebjaar` smallint(3) unsigned DEFAULT NULL,
  `Gebplaats` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

