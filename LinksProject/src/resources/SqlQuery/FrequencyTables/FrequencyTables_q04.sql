-- query 04 
CREATE TABLE IF NOT EXISTS links_prematch.freq_firstname_sex_tmp ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name_str VARCHAR(30) NULL , 
    sex VARCHAR(2) NULL , 
    PRIMARY KEY (id) , 
    INDEX `name_str` (`name_str`) 
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ;
