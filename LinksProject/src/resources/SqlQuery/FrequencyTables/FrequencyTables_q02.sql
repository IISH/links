-- query 02
CREATE  TABLE links_frequency.firstfirstname ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name VARCHAR(30) NULL , 
    frequency INT UNSIGNED NULL , 
    PRIMARY KEY (id) , 
    INDEX `default` (name ASC) 
) ;
