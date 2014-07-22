-- query 01
CREATE TABLE links_frequency.familynames 
    SELECT familyname , COUNT( * ) AS frequency 
    FROM links_cleaned.person_c 
    GROUP BY familyname ;
