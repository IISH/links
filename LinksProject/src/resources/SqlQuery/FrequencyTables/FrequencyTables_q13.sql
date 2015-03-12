-- query 13 
INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , name_int , sex ) 
SELECT firstname2 , firstname2_no , sex 
FROM links_cleaned.person_c 
WHERE firstname2 IS NOT NULL AND firstname2 <> 'null' AND firstname2 <> '' ;
