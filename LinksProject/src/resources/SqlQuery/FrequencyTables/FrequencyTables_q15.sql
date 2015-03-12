-- query 15 
INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , name_int , sex ) 
SELECT firstname4 , firstname4_no , sex 
FROM links_cleaned.person_c 
WHERE firstname4 IS NOT NULL AND firstname4 <> 'null' AND firstname4 <> '' ;
