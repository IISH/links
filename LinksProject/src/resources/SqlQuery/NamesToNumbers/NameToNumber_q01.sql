-- Query 01
UPDATE links_cleaned.person_c, links_prematch.freq_familyname
SET links_cleaned.person_c.familyname_no = links_prematch.freq_familyname.id
WHERE links_cleaned.person_c.familyname = links_prematch.freq_familyname.name ;
