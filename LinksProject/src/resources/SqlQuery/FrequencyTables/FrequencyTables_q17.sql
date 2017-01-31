-- query 17 
INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex ) 
SELECT firstname4 , sex 
FROM links_cleaned.person_c 
WHERE firstname4 IS NOT NULL AND firstname4 <> 'null' AND firstname4 <> '' ;
