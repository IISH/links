-- Query 03
UPDATE links_cleaned.person_c, links_frequency.firstname SET firstname2_no = id WHERE links_cleaned.person_c.firstname2 = links_frequency.firstname.name ;
