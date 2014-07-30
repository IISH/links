-- query 15
CREATE TABLE links_frequency.sex_firstnames 
    SELECT sex , name , COUNT(*) AS frequency 
    FROM links_frequency.subnames 
    GROUP BY sex , name ;
