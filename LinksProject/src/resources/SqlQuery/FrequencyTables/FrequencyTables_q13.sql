-- query 13
INSERT INTO links_frequency.freq_firstnames ( name, frequency )
SELECT name , COUNT(*) AS frequency 
FROM links_frequency.freq_firstnames_sex_tmp 
GROUP BY name ;
