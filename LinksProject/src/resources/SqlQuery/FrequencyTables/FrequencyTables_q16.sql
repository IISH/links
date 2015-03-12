-- query 16 
INSERT INTO links_prematch.freq_firstname ( name_str , name_int , frequency )
SELECT name_str , name_int , COUNT(*) AS frequency 
FROM links_prematch.freq_firstname_sex_tmp 
GROUP BY name_str ;
