-- query 10
INSERT INTO links_frequency.subnames( name ) 
    SELECT firstname4 FROM links_cleaned.person_c ;
