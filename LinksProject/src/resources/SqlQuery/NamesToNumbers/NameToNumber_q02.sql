-- Query 02
UPDATE links_cleaned.person_c, links_prematch.freq_firstnames 
SET firstname1_no = links_prematch.freq_firstnames.id 
WHERE links_cleaned.person_c.firstname1 = links_prematch.freq_firstnames.name ;
