-- query 02 
CREATE TABLE IF NOT EXISTS links_prematch.freq_firstname ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name_str VARCHAR(30) NULL , 
    name_int INT(11) NULL , 
    frequency INT UNSIGNED NULL , 
    PRIMARY KEY (id) , 
    INDEX `name_str` (name_str ASC) ,
    INDEX `name_int` (name_int) 
) ;
