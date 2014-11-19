-- Query 04
UPDATE links_cleaned.person_c, links_frequency.freq_firstnames 
SET firstname3_no = links_frequency.freq_firstnames.id 
WHERE links_cleaned.person_c.firstname3 = links_frequency.freq_firstnames.name ;
