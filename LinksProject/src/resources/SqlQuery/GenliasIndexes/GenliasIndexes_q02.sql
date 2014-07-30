-- Query 2 

ALTER IGNORE TABLE <DBPREFIX>analyse_persoon_items
CHANGE COLUMN `ana_id` `ana_id` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `aty_id` `aty_id` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `atr_volg_nr` `atr_volg_nr` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `ite_id` `ite_id` DECIMAL(12) NULL DEFAULT NULL ,
ADD INDEX `default` (`ana_id` ASC, `aty_id` ASC, `atr_volg_nr` ASC, `ite_id` ASC) ;