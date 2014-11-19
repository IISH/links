-- query 04
CREATE TABLE IF NOT EXISTS links_frequency.freq_familyname ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name VARCHAR(100) NULL , 
    frequency INT UNSIGNED NULL , 
    PRIMARY KEY (id) , 
    INDEX `default` (name ASC) 
) ;
