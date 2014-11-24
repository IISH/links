-- Query 05
UPDATE links_cleaned.person_c, links_prematch.freq_firstnames 
SET firstname4_no = links_prematch.freq_firstnames.id 
WHERE links_cleaned.person_c.firstname4 = links_prematch.freq_firstnames.name ;
