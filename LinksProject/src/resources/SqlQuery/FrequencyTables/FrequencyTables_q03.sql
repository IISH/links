-- query 03 
CREATE TABLE IF NOT EXISTS links_prematch.freq_firstname_sex ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name_str VARCHAR(30) NULL , 
    name_int INT(11) NULL , 
    sex VARCHAR(2) NULL , 
    frequency INT UNSIGNED NULL , 
    PRIMARY KEY (id) , 
    INDEX `name_str` (name_str ASC) , 
    INDEX `name_int` (name_int) 
) ;
