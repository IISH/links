-- query 07
INSERT INTO links_frequency.subnames( name ) 
    SELECT firstname1 FROM links_cleaned.person_c ;
