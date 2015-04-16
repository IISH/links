-- query 11 
INSERT INTO links_prematch.freq_familyname( name_str , frequency ) 
SELECT familyname , COUNT(*) AS frequency 
FROM links_cleaned.person_c 
WHERE familyname IS NOT NULL AND familyname <> 'null' AND familyname <> '' 
GROUP BY familyname ;
