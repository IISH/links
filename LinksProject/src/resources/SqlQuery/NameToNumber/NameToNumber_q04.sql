-- Query 04
UPDATE links_cleaned.person_c, links_frequency.firstname SET firstname3_no = id WHERE links_cleaned.person_c.firstname3 = links_frequency.firstname.name ;
