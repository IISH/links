-- query 22
INSERT INTO links_frequency.firstname( name , frequency ) 
    SELECT name , frequency FROM links_frequency.firstnames ;
