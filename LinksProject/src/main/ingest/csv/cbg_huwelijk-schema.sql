DROP TABLE IF EXISTS `cbg_huwelijk`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cbg_huwelijk` (
	`id` int(11) unsigned NOT NULL,
	
	`Bruidegom_voornaam`               VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_patroniem`              VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_tussenvoegsel`          VARCHAR(  30) DEFAULT NULL,
	`Bruidegom_achternaam`             VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_woonplaats`             VARCHAR( 120) DEFAULT NULL,
	`Bruidegom_leeftijd_literal`       VARCHAR( 300) DEFAULT NULL,
	`Bruidegom_leeftijd_Jaar`          VARCHAR(  10) DEFAULT NULL,
	
	`Bruidegom_datum_geboorte_literal` VARCHAR( 300) DEFAULT NULL,
	`Bruidegom_datum_geboorte_YYYY`    VARCHAR(  50) DEFAULT NULL,
	`Bruidegom_datum_geboorte_MM`      VARCHAR(  50) DEFAULT NULL,
	`Bruidegom_datum_geboorte_DD`      VARCHAR(  50) DEFAULT NULL,
	
	`Bruidegom_geboorteplaats`         VARCHAR( 120) DEFAULT NULL,
	`Bruidegom_beroep1`                VARCHAR( 200) DEFAULT NULL,
	`Bruidegom_beroep2`                VARCHAR( 200) DEFAULT NULL,
	
	`Bruidegom_vader_voornaam`         VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_vader_patroniem`        VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_vader_tussenvoegsel`    VARCHAR(  30) DEFAULT NULL,
	`Bruidegom_vader_achternaam`       VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_vader_beroep1`          VARCHAR( 200) DEFAULT NULL,
	`Bruidegom_vader_beroep2`          VARCHAR( 200) DEFAULT NULL,
	
	`Bruidegom_moeder_voornaam`        VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_moeder_patroniem`       VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_moeder_tussenvoegsel`   VARCHAR(  30) DEFAULT NULL,
	`Bruidegom_moeder_achternaam`      VARCHAR( 100) DEFAULT NULL,
	`Bruidegom_moeder_beroep1`         VARCHAR( 200) DEFAULT NULL,
	`Bruidegom_moeder_beroep2`         VARCHAR( 200) DEFAULT NULL,
	
	`Bruid_voornaam`                   VARCHAR( 100) DEFAULT NULL,
	`Bruid_patroniem`                  VARCHAR( 100) DEFAULT NULL,
	`Bruid_tussenvoegsel`              VARCHAR(  30) DEFAULT NULL,
	`Bruid_achternaam`                 VARCHAR( 100) DEFAULT NULL,
	`Bruid_woonplaats`                 VARCHAR( 120) DEFAULT NULL,
	`Bruid_leeftijd_literal`           VARCHAR( 300) DEFAULT NULL,
	`Bruid_leeftijd_Jaar`              VARCHAR(  10) DEFAULT NULL,
	
	`Bruid_datum_geboorte_literal`     VARCHAR( 300) DEFAULT NULL,
	`Bruid_datum_geboorte_YYYY`        VARCHAR(  50) DEFAULT NULL,
	`Bruid_datum_geboorte_MM`          VARCHAR(  50) DEFAULT NULL,
	`Bruid_datum_geboorte_DD`          VARCHAR(  50) DEFAULT NULL,
	
	`Bruid_geboorteplaats`             VARCHAR( 120) DEFAULT NULL,
	`Bruid_beroep1`                    VARCHAR( 200) DEFAULT NULL,
	`Bruid_beroep2`                    VARCHAR( 200) DEFAULT NULL,
	
	`Bruid_vader_voornaam`             VARCHAR( 100) DEFAULT NULL,
	`Bruid_vader_patroniem`            VARCHAR( 100) DEFAULT NULL,
	`Bruid_vader_tussenvoegsel`        VARCHAR(  30) DEFAULT NULL,
	`Bruid_vader_achternaam`           VARCHAR( 100) DEFAULT NULL,
	`Bruid_vader_beroep1`              VARCHAR( 200) DEFAULT NULL,
	`Bruid_vader_beroep2`              VARCHAR( 200) DEFAULT NULL,
	
	`Bruid_moeder_voornaam`            VARCHAR( 100) DEFAULT NULL,
	`Bruid_moeder_patroniem`           VARCHAR( 100) DEFAULT NULL,
	`Bruid_moeder_tussenvoegsel`       VARCHAR(  30) DEFAULT NULL,
	`Bruid_moeder_achternaam`          VARCHAR( 100) DEFAULT NULL,
	`Bruid_moeder_beroep1`             VARCHAR( 200) DEFAULT NULL,
	`Bruid_moeder_beroep2`             VARCHAR( 200) DEFAULT NULL,
	
	`Datum_huwelijk_literal`           VARCHAR( 300) DEFAULT NULL,
	`Datum_huwelijk_YYYY`              VARCHAR(  50) DEFAULT NULL,
	`Datum_huwelijk_MM`                VARCHAR(  50) DEFAULT NULL,
	`Datum_huwelijk_DD`                VARCHAR(  50) DEFAULT NULL,
	`Plaats_huwelijk`                  VARCHAR(  22) DEFAULT NULL,
	
	`Datum_huwelijksaangifte_literal`  VARCHAR( 300) DEFAULT NULL,
	`Datum_huwelijksaangifte_YYYY`     VARCHAR(  50) DEFAULT NULL,
	`Datum_huwelijksaangifte_MM`       VARCHAR(  50) DEFAULT NULL,
	`Datum_huwelijksaangifte_DD`       VARCHAR(  50) DEFAULT NULL,
	
	`Datum_echtscheiding_literal`      VARCHAR( 300) DEFAULT NULL,
	`Datum_echtscheiding_YYYY`         VARCHAR(  50) DEFAULT NULL,
	`Datum_echtscheiding_MM`           VARCHAR(  50) DEFAULT NULL,
	`Datum_echtscheiding_DD`           VARCHAR(  50) DEFAULT NULL,
	
	`Gemeentenaam`                     VARCHAR( 100) DEFAULT NULL,
	
	`Akte_datum_literal`               VARCHAR( 300) DEFAULT NULL,
	`Akte_datum_YYYY`                  VARCHAR(  50) DEFAULT NULL,
	`Akte_datum_MM`                    VARCHAR(  50) DEFAULT NULL,
	`Akte_datum_DD`                    VARCHAR(  50) DEFAULT NULL,
	
	`Bronsoort`                        VARCHAR(  50) DEFAULT NULL,
	`Plaats_instelling`                VARCHAR( 120) DEFAULT NULL,
	`Naam_instelling`                  VARCHAR( 100) DEFAULT NULL,
	`Toegangsnummer`                   VARCHAR(  20) DEFAULT NULL,
	`Inventarisnummer`                 VARCHAR(  10) DEFAULT NULL,
	`Aktenummer`                       VARCHAR(  30) DEFAULT NULL,
	
	`Scan_nummer_1`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_1`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_2`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_2`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_3`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_3`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_4`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_4`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_5`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_5`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_6`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_6`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_7`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_7`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_8`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_8`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_9`                    VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_9`                       VARCHAR( 200) DEFAULT NULL,
	`Scan_nummer_10`                   VARCHAR(  50) DEFAULT NULL,
	`Scan_uri_10`                      VARCHAR( 200) DEFAULT NULL,
	
	`Mutatiedatum`                     VARCHAR(  50) DEFAULT NULL,
	`Scan_URI_Origineel`               VARCHAR( 200) DEFAULT NULL,
	`RecordGUID`                       VARCHAR(  80) DEFAULT NULL,
	`Opmerking`                        VARCHAR(2048) DEFAULT NULL,
	`AkteSoort`                        VARCHAR( 200) DEFAULT NULL,
	
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
