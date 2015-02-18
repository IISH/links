-- Query 03
UPDATE links_cleaned.person_c, links_prematch.freq_firstname 
SET firstname2_no = links_prematch.freq_firstname.id 
WHERE links_cleaned.person_c.firstname2 = links_prematch.freq_firstname.name_str ;
