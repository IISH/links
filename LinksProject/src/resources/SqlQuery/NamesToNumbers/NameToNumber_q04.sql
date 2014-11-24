-- Query 04
UPDATE links_cleaned.person_c, links_prematch.freq_firstnames 
SET firstname3_no = links_prematch.freq_firstnames.id 
WHERE links_cleaned.person_c.firstname3 = links_prematch.freq_firstnames.name ;
