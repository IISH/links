-- query 12 
INSERT INTO links_frequency.freq_firstnames_sex_tmp( name , sex ) 
SELECT firstname4 , sex 
FROM links_cleaned.person_c 
WHERE firstname4 IS NOT NULL AND firstname <> '' ;
