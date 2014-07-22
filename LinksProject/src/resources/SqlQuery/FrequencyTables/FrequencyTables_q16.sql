-- query 16
CREATE TABLE links_frequency.firstnames 
    SELECT name , COUNT(*) AS frequency 
    FROM links_frequency.subnames 
    GROUP BY name ;
