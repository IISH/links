-- Query 04
UPDATE links_cleaned.person_c, links_prematch.freq_firstname 
SET firstname3_no = links_prematch.freq_firstname.id 
WHERE links_cleaned.person_c.firstname3 = links_prematch.freq_firstname.name_str ;
