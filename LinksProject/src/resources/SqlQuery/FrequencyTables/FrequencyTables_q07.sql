-- query 07 
CREATE TABLE IF NOT EXISTS links_prematch.freq_firstnames_sex_tmp ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name VARCHAR(30) NULL , 
    sex VARCHAR(2) NULL , 
    PRIMARY KEY (id) , 
    INDEX `default` (name ASC) 
) ;
