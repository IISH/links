DROP TABLE IF EXISTS `cbg_geboorte`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cbg_geboorte` (
	`id` int(11) unsigned NOT NULL,

	`Kind_voornaam`               VARCHAR( 100) DEFAULT NULL,
	`Kind_patroniem`              VARCHAR( 100) DEFAULT NULL,
	`Kind_tussenvoegsel`          VARCHAR(  30) DEFAULT NULL,
	`Kind_achternaam`             VARCHAR( 100) DEFAULT NULL,
	`Kind_geslacht`               VARCHAR(  15) DEFAULT NULL,
	`Kind_woonadres`              VARCHAR( 120) DEFAULT NULL,
	
	`Kind_datum_geboorte_literal` VARCHAR( 300) DEFAULT NULL,
	`Kind_datum_geboorte_YYYY`    VARCHAR(  50) DEFAULT NULL,
	`Kind_datum_geboorte_MM`      VARCHAR(  50) DEFAULT NULL,
	`Kind_datum_geboorte_DD`      VARCHAR(  50) DEFAULT NULL,
	
	`Vondeling`                   VARCHAR(  10) DEFAULT NULL,
	`Opmerking_kind`              VARCHAR( 500) DEFAULT NULL,
	
	`Vader_voornaam`              VARCHAR( 100) DEFAULT NULL,
	`Vader_patroniem`             VARCHAR( 100) DEFAULT NULL,
	`Vader_tussenvoegsel`         VARCHAR(  30) DEFAULT NULL,
	`Vader_achternaam`            VARCHAR( 100) DEFAULT NULL,
	`Vader_leeftijd_literal`      VARCHAR( 300) DEFAULT NULL,
	`Vader_leeftijd_Jaar`         VARCHAR(  10) DEFAULT NULL,
	`Vader_beroep1`               VARCHAR( 200) DEFAULT NULL,
	`Vader_beroep2`               VARCHAR( 200) DEFAULT NULL,
	
	`Moeder_voornaam`             VARCHAR( 100) DEFAULT NULL,
	`Moeder_patroniem`            VARCHAR( 100) DEFAULT NULL,
	`Moeder_tussenvoegsel`        VARCHAR(  30) DEFAULT NULL,
	`Moeder_achternaam`           VARCHAR( 100) DEFAULT NULL,
	`Moeder_leeftijd_literal`     VARCHAR( 300) DEFAULT NULL,
	`Moeder_leeftijd_Jaar`        VARCHAR(  10) DEFAULT NULL,
	`Moeder_beroep1`              VARCHAR( 200) DEFAULT NULL,
	`Moeder_beroep2`              VARCHAR( 200) DEFAULT NULL,
	
	`Kind_datum_doop_literal`     VARCHAR( 300) DEFAULT NULL,
	`Kind_datum_doop_YYYY`        VARCHAR(  50) DEFAULT NULL,
	`Kind_datum_doop_MM`          VARCHAR(  50) DEFAULT NULL,
	`Kind_datum_doop_DD`          VARCHAR(  50) DEFAULT NULL,
	
	`Kind_plaats_geboren`         VARCHAR( 120) DEFAULT NULL,
	
	`Gemeentenaam`                VARCHAR( 100) DEFAULT NULL,
	
	`Akte_datum_literal`          VARCHAR( 300) DEFAULT NULL,
	`Akte_datum_YYYY`             VARCHAR(  50) DEFAULT NULL,
	`Akte_datum_MM`               VARCHAR(  50) DEFAULT NULL,
	`Akte_datum_DD`               VARCHAR(  50) DEFAULT NULL,
	
	`Bronsoort`                   VARCHAR(  50) DEFAULT NULL,
	`Plaats_instelling`           VARCHAR( 120) DEFAULT NULL,
	`Naam_instelling`             VARCHAR( 100) DEFAULT NULL,
	`Toegangsnummer`              VARCHAR(  20) DEFAULT NULL,
	`Inventarisnummer`            VARCHAR(  10) DEFAULT NULL,
	`Aktenummer`                  VARCHAR(  30) DEFAULT NULL,
	
	`Scan_nummer_1`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_1`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_2`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_2`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_3`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_3`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_4`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_4`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_5`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_5`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_6`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_6`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_7`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_7`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_8`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_8`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_9`               VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_9`                  VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_10`              VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_10`                 VARCHAR( 200) DEFAULT NULL,
	
	`Mutatiedatum`                VARCHAR(  50) DEFAULT NULL,
	`Scan_URI_Origineel`          VARCHAR( 200) DEFAULT NULL,
	`RecordGUID`                  VARCHAR(  80) DEFAULT NULL,
	`Opmerking`                   VARCHAR(2048) DEFAULT NULL,
	`AkteSoort`                   VARCHAR( 200) DEFAULT NULL,
	
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
