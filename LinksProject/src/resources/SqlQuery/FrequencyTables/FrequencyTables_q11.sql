-- query 11 
INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , name_int , sex ) 
SELECT firstname3 , firstname3_no , sex 
FROM links_cleaned.person_c 
WHERE firstname3 IS NOT NULL AND firstname <> '' ;
