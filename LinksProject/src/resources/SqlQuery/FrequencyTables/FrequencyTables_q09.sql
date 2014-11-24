-- query 09 
INSERT INTO links_prematch.freq_firstnames_sex_tmp( name , sex ) 
SELECT firstname1, sex 
FROM links_cleaned.person_c 
WHERE firstname1 IS NOT NULL AND firstname <> '' ;
