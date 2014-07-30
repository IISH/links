-- query 23
INSERT INTO links_frequency.sex_firstname( name , sex , frequency ) 
    SELECT name , sex , frequency FROM links_frequency.sex_firstnames ;
