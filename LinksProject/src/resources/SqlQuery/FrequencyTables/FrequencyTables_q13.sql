-- query 13
INSERT INTO links_frequency.subnames( name , sex ) 
    SELECT firstname3, sex FROM links_cleaned.person_c ;
