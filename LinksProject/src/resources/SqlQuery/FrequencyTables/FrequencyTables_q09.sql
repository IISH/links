-- query 09 
INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , name_int , sex ) 
SELECT firstname1 , firstname1_no , sex 
FROM links_cleaned.person_c 
WHERE firstname1 IS NOT NULL AND firstname1 <> 'null' AND firstname1 <> '' ;
