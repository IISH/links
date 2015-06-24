UPDATE links_general.ref_familyname , links_frequency.familyname_a
SET
links_general.ref_familyname.standard           = links_frequency.familyname_a.name_b ,
links_general.ref_familyname.standard_code      = 'o' ,
links_general.ref_familyname.standard_source    = 'LS1'
WHERE
links_general.ref_familyname.original           = links_frequency.familyname_a.name_1 AND
links_general.ref_familyname.standard_code      = 'x' AND
links_general.ref_familyname.standard           IS NULL AND
links_frequency.familyname_a.name_b IS NOT NULL ;

UPDATE links_general.ref_firstname , links_frequency.firstname_a
SET
links_general.ref_firstname.standard           = links_frequency.firstname_a.name_b ,
links_general.ref_firstname.standard_code      = 'o' ,
links_general.ref_firstname.standard_source    = 'LS1'
WHERE
links_general.ref_firstname.original           = links_frequency.firstname_a.name_1 AND
links_general.ref_firstname.standard_code      = 'x' AND
links_general.ref_firstname.standard           IS NULL AND
links_frequency.firstname_a.name_b IS NOT NULL 