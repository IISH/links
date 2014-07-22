-- query 04
CREATE TABLE links_frequency.prefix_familyname 
    SELECT prefix, familyname, COUNT(*) AS frequency 
    FROM links_cleaned.person_c 
    GROUP BY prefix, familyname ;
