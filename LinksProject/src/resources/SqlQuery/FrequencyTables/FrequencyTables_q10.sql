-- query 10
CREATE  TABLE links_frequency.subnames ( 
    id INT UNSIGNED NOT NULL AUTO_INCREMENT , 
    name VARCHAR(30) NULL , 
    sex VARCHAR(2) NULL , 
    PRIMARY KEY (id) , 
    INDEX `default` (name ASC) 
) ;
