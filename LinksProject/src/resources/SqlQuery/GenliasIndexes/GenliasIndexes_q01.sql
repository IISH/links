-- Query 1

ALTER IGNORE TABLE <DBPREFIX>analyse_personen
CHANGE COLUMN `ana_id` `ana_id` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `arl_id` `arl_id` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `aty_id` `aty_id` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `atr_volg_nr` `atr_volg_nr` DECIMAL(12) NULL DEFAULT NULL  ,
ADD INDEX `default` (`ana_id` ASC, `arl_id` ASC, `aty_id` ASC) ;  