-- Query 01
UPDATE person_c, familyname
SET
person_c.familyname_no = familyname.id
WHERE
person_c.familyname = familyname.name ;
