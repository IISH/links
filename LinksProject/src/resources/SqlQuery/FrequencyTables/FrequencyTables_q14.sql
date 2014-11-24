-- query 14 
INSERT INTO links_prematch.freq_firstnames_sex ( name, sex, frequency )
SELECT name , sex, COUNT(*) AS frequency 
FROM links_prematch.freq_firstnames_sex_tmp 
GROUP BY sex , name ;
