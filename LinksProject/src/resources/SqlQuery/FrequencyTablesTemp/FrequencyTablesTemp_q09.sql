-- query 09
INSERT INTO links_frequency.subnames( name ) 
    SELECT firstname3 FROM links_cleaned.person_c ;
