-- query 12 
INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex ) 
SELECT firstname1 , sex 
FROM links_cleaned.person_c 
WHERE firstname1 IS NOT NULL AND firstname1 <> 'null' AND firstname1 <> '' ;
