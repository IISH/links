UPDATE links_prematch.links_base , links_cleaned.person_c 
SET 
partner_id                = links_cleaned.person_c.id_person , 
partner_familyname_fc     = LEFT( links_cleaned.person_c.familyname, 1) , 
partner_familyname_prefix = links_cleaned.person_c.prefix , 
partner_familyname_str    = links_cleaned.person_c.familyname , 
partner_familyname        = links_cleaned.person_c.familyname_no , 
partner_firstname         = links_cleaned.person_c.firstname , 
partner_firstname1_str    = links_cleaned.person_c.firstname1 , 
partner_firstname1        = links_cleaned.person_c.firstname1_no , 
partner_firstname2        = links_cleaned.person_c.firstname2_no , 
partner_firstname3        = links_cleaned.person_c.firstname3_no , 
partner_firstname4        = links_cleaned.person_c.firstname4_no , 
partner_sex               = links_cleaned.person_c.sex , 
partner_birth_min         = links_cleaned.person_c.birth_min_days , 
partner_birth_max         = links_cleaned.person_c.birth_max_days , 
partner_birth_loc         = links_cleaned.person_c.birth_location , 
partner_marriage_min      = links_cleaned.person_c.mar_min_days , 
partner_marriage_max      = links_cleaned.person_c.mar_max_days , 
partner_marriage_loc      = links_cleaned.person_c.mar_location , 
partner_death_min         = links_cleaned.person_c.death_min_days , 
partner_death_max         = links_cleaned.person_c.death_max_days , 
partner_death_loc         = links_cleaned.person_c.death_location 

WHERE 
links_prematch.links_base.id_registration = links_cleaned.person_c.id_registration AND 
(
 ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 2 AND links_prematch.links_base.ego_role = 3 ) OR 
 ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 3 AND links_prematch.links_base.ego_role = 2 ) OR 

 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 4 AND links_prematch.links_base.ego_role = 7 ) OR 
 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 7 AND links_prematch.links_base.ego_role = 4 ) OR 
 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 5 AND links_prematch.links_base.ego_role = 6 ) OR 
 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 6 AND links_prematch.links_base.ego_role = 5 ) OR 
 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 8 AND links_prematch.links_base.ego_role = 9 ) OR 
 ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 9 AND links_prematch.links_base.ego_role = 8 ) OR 

 ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role =  2 AND links_prematch.links_base.ego_role =  3 ) OR 
 ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role =  3 AND links_prematch.links_base.ego_role =  2 ) OR 
 ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 10 AND links_prematch.links_base.ego_role = 11 ) OR 
 ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 11 AND links_prematch.links_base.ego_role = 10 ) 
) ; 
