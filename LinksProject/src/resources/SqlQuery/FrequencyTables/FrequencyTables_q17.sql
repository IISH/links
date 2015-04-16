-- query 17 
INSERT INTO links_prematch.freq_firstname_sex ( name_str , sex, frequency )
SELECT name_str , sex, COUNT(*) AS frequency 
FROM links_prematch.freq_firstname_sex_tmp 
GROUP BY sex , name_str ;
