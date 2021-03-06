UPDATE links_prematch.links_base , links_cleaned.person_c 
SET 
father_id                = links_cleaned.person_c.id_person , 
father_familyname_fc     = LEFT( links_cleaned.person_c.familyname, 1) , 
father_familyname_prefix = links_cleaned.person_c.prefix , 
father_familyname_str    = links_cleaned.person_c.familyname , 
father_familyname        = links_cleaned.person_c.familyname_no , 
father_firstname         = links_cleaned.person_c.firstname , 
father_firstname1_str    = links_cleaned.person_c.firstname1 , 
father_firstname1        = links_cleaned.person_c.firstname1_no , 
father_firstname2        = links_cleaned.person_c.firstname2_no , 
father_firstname3        = links_cleaned.person_c.firstname3_no , 
father_firstname4        = links_cleaned.person_c.firstname4_no , 
father_sex               = links_cleaned.person_c.sex , 
father_birth_min         = links_cleaned.person_c.birth_min_days , 
father_birth_max         = links_cleaned.person_c.birth_max_days , 
father_birth_loc         = links_cleaned.person_c.birth_location , 
father_marriage_min      = links_cleaned.person_c.mar_min_days , 
father_marriage_max      = links_cleaned.person_c.mar_max_days , 
father_marriage_loc      = links_cleaned.person_c.mar_location , 
father_death_min         = links_cleaned.person_c.death_min_days , 
father_death_max         = links_cleaned.person_c.death_max_days , 
father_death_loc         = links_cleaned.person_c.death_location 

WHERE 
links_prematch.links_base.id_registration = links_cleaned.person_c.id_registration AND 
(
 ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 3 AND links_prematch.links_base.ego_role =  1 ) OR 

 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 6 AND links_prematch.links_base.ego_role =  4 ) OR 
 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 9 AND links_prematch.links_base.ego_role =  7 ) OR 

 ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 3 AND links_prematch.links_base.ego_role = 10 ) 
) ; 
