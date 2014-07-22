-- Create and alter tables 
CREATE TABLE familyname_a 
    SELECT * FROM familyname WHERE frequency < 3 ;


CREATE TABLE familyname_b 
    SELECT * FROM familyname WHERE frequency > 9 ;


CREATE TABLE firstname_a 
    SELECT * FROM firstname WHERE frequency < 3 ;


CREATE TABLE firstname_b 
    SELECT * FROM firstname WHERE frequency > 9 ;



ALTER TABLE `links_frequency`.`familyname_a` 
ADD PRIMARY KEY (`id`) ,
ADD COLUMN `name_2` VARCHAR(100) NULL  AFTER `name_1` , 
ADD COLUMN `name_b` VARCHAR(100) NULL  AFTER `frequency` , 
CHANGE COLUMN `name` `name_1` VARCHAR(100) NULL DEFAULT NULL  ;


ALTER TABLE `links_frequency`.`firstname_a` 
ADD PRIMARY KEY (`id`) ,
ADD COLUMN `name_2` VARCHAR(100) NULL  AFTER `name_1` , 
ADD COLUMN `name_b` VARCHAR(100) NULL  AFTER `frequency` , 
CHANGE COLUMN `name` `name_1` VARCHAR(100) NULL DEFAULT NULL  ;


ALTER TABLE `links_frequency`.`familyname_b` 
ADD PRIMARY KEY (`id`) ,
ADD COLUMN `name_2` VARCHAR(100) NULL  AFTER `name_1` , 
ADD COLUMN `first_char` VARCHAR(1) NULL  AFTER `frequency` , 
CHANGE COLUMN `name` `name_1` VARCHAR(100) NULL DEFAULT NULL  ;


ALTER TABLE `links_frequency`.`firstname_b` 
ADD PRIMARY KEY (`id`) ,
ADD COLUMN `name_2` VARCHAR(100) NULL  AFTER `name_1` , 
ADD COLUMN `first_char` VARCHAR(1) NULL  AFTER `frequency` , 
CHANGE COLUMN `name` `name_1` VARCHAR(100) NULL DEFAULT NULL
