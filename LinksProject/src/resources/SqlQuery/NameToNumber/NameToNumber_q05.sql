-- Query 05
UPDATE links_cleaned.person_c, links_frequency.firstname SET firstname4_no = id WHERE links_cleaned.person_c.firstname4 = links_frequency.firstname.name ;
