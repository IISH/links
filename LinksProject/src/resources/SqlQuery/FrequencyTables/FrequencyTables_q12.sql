-- query 12
INSERT INTO links_frequency.subnames( name , sex ) 
    SELECT firstname2, sex FROM links_cleaned.person_c ;
