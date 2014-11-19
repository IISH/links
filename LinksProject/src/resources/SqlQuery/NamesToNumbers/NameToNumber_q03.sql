-- Query 03
UPDATE links_cleaned.person_c, links_frequency.freq_firstnames 
SET firstname2_no = links_frequency.freq_firstnames.id 
WHERE links_cleaned.person_c.firstname2 = links_frequency.freq_firstnames.name ;
