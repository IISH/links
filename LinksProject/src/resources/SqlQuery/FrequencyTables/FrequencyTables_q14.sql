-- query 14
INSERT INTO links_frequency.subnames( name , sex ) 
    SELECT firstname4 , sex FROM links_cleaned.person_c ;
