-- query 04
INSERT INTO links_frequency.familyname( name , frequency ) 
    SELECT familyname, frequency FROM links_frequency.familynames ; 
