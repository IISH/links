UPDATE links_prematch.links_base , links_cleaned.person_c
SET 
mother_id                = links_cleaned.person_c.id_person , 
mother_familyname_fc     = LEFT( links_cleaned.person_c.familyname, 1) , 
mother_familyname_prefix = links_cleaned.person_c.prefix , 
mother_familyname_str    = links_cleaned.person_c.familyname , 
mother_familyname        = links_cleaned.person_c.familyname_no , 
mother_firstname         = links_cleaned.person_c.firstname , 
mother_firstname1_str    = links_cleaned.person_c.firstname1 , 
mother_firstname1        = links_cleaned.person_c.firstname1_no , 
mother_firstname2        = links_cleaned.person_c.firstname2_no , 
mother_firstname3        = links_cleaned.person_c.firstname3_no , 
mother_firstname4        = links_cleaned.person_c.firstname4_no , 
mother_sex               = links_cleaned.person_c.sex , 
mother_birth_min         = links_cleaned.person_c.birth_min_days , 
mother_birth_max         = links_cleaned.person_c.birth_max_days , 
mother_birth_loc         = links_cleaned.person_c.birth_location , 
mother_marriage_min      = links_cleaned.person_c.mar_min_days , 
mother_marriage_max      = links_cleaned.person_c.mar_max_days , 
mother_marriage_loc      = links_cleaned.person_c.mar_location , 
mother_death_min         = links_cleaned.person_c.death_min_days , 
mother_death_max         = links_cleaned.person_c.death_max_days , 
mother_death_loc         = links_cleaned.person_c.death_location 

WHERE 
links_prematch.links_base.id_registration = links_cleaned.person_c.id_registration AND 
( 
 ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 2 AND links_prematch.links_base.ego_role = 1 ) OR 

 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 5 AND links_prematch.links_base.ego_role = 4 ) OR 
 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 8 AND links_prematch.links_base.ego_role = 7 ) OR 

 ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 2 AND links_prematch.links_base.ego_role = 10 )
) ; 
