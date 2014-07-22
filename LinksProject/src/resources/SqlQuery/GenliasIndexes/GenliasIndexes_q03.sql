-- Query 3

ALTER IGNORE TABLE <DBPREFIX>analyse_items
CHANGE COLUMN `ana_id` `ana_id` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `aty_id` `aty_id` DECIMAL(12) NULL DEFAULT NULL ,
CHANGE COLUMN `ite_id` `ite_id` DECIMAL(12) NULL DEFAULT NULL ,
ADD INDEX `default` (`ana_id` ASC, `ite_id` ASC) ;