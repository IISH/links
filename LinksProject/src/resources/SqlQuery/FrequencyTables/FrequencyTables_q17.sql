-- query 17 
INSERT INTO links_prematch.freq_firstname_sex ( name_str , name_int , sex, frequency )
SELECT name_str , name_int , sex, COUNT(*) AS frequency 
FROM links_prematch.freq_firstname_sex_tmp 
GROUP BY sex , name_str ;
