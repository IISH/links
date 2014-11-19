-- query 06
CREATE TABLE IF NOT EXISTS links_frequency.freq_firstnames_sex ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name VARCHAR(30) NULL , 
    sex VARCHAR(2) NULL , 
    frequency INT UNSIGNED NULL , 
    PRIMARY KEY (id) , 
    INDEX `default` (name ASC) 
) ;
