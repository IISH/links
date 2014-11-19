-- query 11 
INSERT INTO links_frequency.freq_firstnames_sex_tmp( name , sex ) 
SELECT firstname3, sex 
FROM links_cleaned.person_c 
WHERE firstname3 IS NOT NULL AND firstname <> '' ;
