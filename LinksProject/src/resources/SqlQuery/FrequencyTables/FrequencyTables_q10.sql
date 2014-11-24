-- query 10 
INSERT INTO links_prematch.freq_firstnames_sex_tmp( name , sex ) 
SELECT firstname2, sex 
FROM links_cleaned.person_c 
WHERE firstname2 IS NOT NULL AND firstname <> '' ;
