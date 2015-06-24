-- Query 02
UPDATE links_cleaned.person_c, links_prematch.freq_firstname 
SET firstname1_no = links_prematch.freq_firstname.id 
WHERE links_cleaned.person_c.firstname1 = links_prematch.freq_firstname.name_str ;
