-- query 08
INSERT INTO links_frequency.subnames( name ) 
    SELECT firstname2 FROM links_cleaned.person_c ;
