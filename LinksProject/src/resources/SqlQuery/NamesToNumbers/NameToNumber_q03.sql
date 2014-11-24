-- Query 03
UPDATE links_cleaned.person_c, links_prematch.freq_firstnames 
SET firstname2_no = links_prematch.freq_firstnames.id 
WHERE links_cleaned.person_c.firstname2 = links_prematch.freq_firstnames.name ;
