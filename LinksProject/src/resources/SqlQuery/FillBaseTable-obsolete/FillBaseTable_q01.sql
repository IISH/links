INSERT INTO links_prematch.links_base 
( 
id_registration , 
id_source , 
id_persist_registration , 
registration_maintype , 
registration_type , 
extract , 
registration_days , 
registration_location , 
ego_id , 
ego_familyname_fc , 
ego_familyname_prefix , 
ego_familyname_str , 
ego_familyname , 
ego_firstname , 
ego_firstname1_str , 
ego_firstname1 , 
ego_firstname2 , 
ego_firstname3 , 
ego_firstname4 , 
ego_sex , 
ego_birth_min , 
ego_birth_max , 
ego_birth_loc , 
ego_marriage_min , 
ego_marriage_max , 
ego_marriage_loc , 
ego_death_min , 
ego_death_max , 
ego_death_loc , 
ego_role 
) 
SELECT 
links_cleaned.registration_c.id_registration , 
links_cleaned.registration_c.id_source , 
links_cleaned.registration_c.id_persist_registration , 
links_cleaned.registration_c.registration_maintype , 
links_cleaned.registration_c.registration_type , 
links_cleaned.registration_c.extract , 
links_cleaned.registration_c.registration_days , 
links_cleaned.registration_c.registration_location_no , 
links_cleaned.person_c.id_person , 
LEFT( links_cleaned.person_c.familyname, 1 ), 
links_cleaned.person_c.prefix , 
links_cleaned.person_c.familyname , 
links_cleaned.person_c.familyname_no , 
links_cleaned.person_c.firstname , 
links_cleaned.person_c.firstname1 , 
links_cleaned.person_c.firstname1_no , 
links_cleaned.person_c.firstname2_no , 
links_cleaned.person_c.firstname3_no , 
links_cleaned.person_c.firstname4_no , 
links_cleaned.person_c.sex , 
links_cleaned.person_c.birth_min_days , 
links_cleaned.person_c.birth_max_days , 
links_cleaned.person_c.birth_location , 
links_cleaned.person_c.mar_min_days , 
links_cleaned.person_c.mar_max_days , 
links_cleaned.person_c.mar_location , 
links_cleaned.person_c.death_min_days , 
links_cleaned.person_c.death_max_days , 
links_cleaned.person_c.death_location , 
links_cleaned.person_c.role 
FROM links_cleaned.registration_c , links_cleaned.person_c
WHERE links_cleaned.registration_c.id_registration = links_cleaned.person_c.id_registration 
AND links_cleaned.registration_c.not_linksbase IS NULL 
AND links_cleaned.person_c.not_linksbase_p IS NULL 
AND (
 ( links_cleaned.registration_c.registration_maintype = 1 AND ( 
    links_cleaned.person_c.role = 1 OR 
    links_cleaned.person_c.role = 2 OR 
    links_cleaned.person_c.role = 3 ) 
 ) OR 
 ( links_cleaned.registration_c.registration_maintype = 2 AND ( 
    links_cleaned.person_c.role = 4 OR 
    links_cleaned.person_c.role = 5 OR 
    links_cleaned.person_c.role = 6 OR 
    links_cleaned.person_c.role = 7 OR 
    links_cleaned.person_c.role = 8 OR 
    links_cleaned.person_c.role = 9 ) 
 ) OR 
 ( links_cleaned.registration_c.registration_maintype = 3 AND ( 
    links_cleaned.person_c.role =  2 OR 
    links_cleaned.person_c.role =  3 OR 
    links_cleaned.person_c.role = 10 OR 
    links_cleaned.person_c.role = 11 ) 
 ) 
) ; 
