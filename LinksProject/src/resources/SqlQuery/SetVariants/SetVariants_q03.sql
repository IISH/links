-- set first char
UPDATE links_frequency.familyname_b 
    SET first_char = SUBSTRING( name_2 , 1 , 1 ) ;


UPDATE links_frequency.firstname_b 
    SET first_char = SUBSTRING( name_2 , 1 , 1 ) 