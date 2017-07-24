DROP TABLE IF EXISTS `militieregisters`;
CREATE TABLE `militieregisters` (
	`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
	`record_id` varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`file_id`   varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`coords`    varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`given`     varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`prefix`    varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`surname`   varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`ev_type`   varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`location`  varchar(100) COLLATE utf8_bin DEFAULT NULL,
	`date`      varchar(100) COLLATE utf8_bin DEFAULT NULL,
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
