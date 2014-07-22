-- query 11
INSERT INTO links_frequency.subnames(name,sex) 
    SELECT firstname1, sex FROM links_cleaned.person_c ;
