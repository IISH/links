-- Query 01
UPDATE links_cleaned.person_c, links_frequency.familyname
SET
links_cleaned.person_c.familyname_no = links_frequency.familyname.id
WHERE
links_cleaned.person_c.familyname = links_frequency.familyname.name ;
