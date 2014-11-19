-- query 08 
INSERT INTO links_frequency.freq_familyname( name , frequency ) 
SELECT familyname, COUNT(*) AS frequency 
FROM links_cleaned.person_c 
WHERE familyname IS NOT NULL AND familyname <> '' 
GROUP BY familyname ;
