-- query 01
CREATE TABLE links_frequency.sex_one_firstname 
    SELECT firstname1, sex, COUNT(*) AS frequency 
    FROM links_cleaned.person_c 
    GROUP BY firstname1, sex ;
