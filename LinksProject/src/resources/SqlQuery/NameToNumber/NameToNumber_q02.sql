-- Query 02
UPDATE links_cleaned.person_c, links_frequency.firstname SET firstname1_no = id WHERE links_cleaned.person_c.firstname1 = links_frequency.firstname.name ;
