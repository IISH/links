-- Query 05
UPDATE links_cleaned.person_c, links_frequency.freq_firstnames 
SET firstname4_no = links_frequency.freq_firstnames.id 
WHERE links_cleaned.person_c.firstname4 = links_frequency.freq_firstnames.name ;
