-- Replace the first letter Copy name1 to name 2 and change C to S etc..
UPDATE links_frequency.familyname_a 
SET name_2 = name_1 ;

UPDATE links_frequency.familyname_b 
SET name_2 = name_1 ;

UPDATE links_frequency.firstname_a 
SET name_2 = name_1 ;

UPDATE links_frequency.firstname_b 
SET name_2 = name_1 ;


UPDATE links_frequency.familyname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 2 ) , 'ij' , 'y' ) , SUBSTRING( name_2 , 3 ) ) 
WHERE SUBSTRING( name_2 , 1 , 2 ) = 'ij' ;

UPDATE links_frequency.familyname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'c' , 'k' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'c' ;

UPDATE links_frequency.familyname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 's' , 'z' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 's' ;

UPDATE links_frequency.familyname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'i' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'i' ;

UPDATE links_frequency.familyname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'j' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'j' ;


UPDATE links_frequency.familyname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 2 ) , 'ij' , 'y' ) , SUBSTRING( name_2 , 3 ) ) 
WHERE SUBSTRING( name_2 , 1 , 2 ) = 'ij' ;

UPDATE links_frequency.familyname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'c' , 'k' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'c' ;

UPDATE links_frequency.familyname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 's' , 'z' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 's' ;

UPDATE links_frequency.familyname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'i' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'i' ;

UPDATE links_frequency.familyname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'j' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'j' ;


UPDATE links_frequency.firstname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 2 ) , 'ij' , 'y' ) , SUBSTRING( name_2 , 3 ) ) 
WHERE SUBSTRING( name_2 , 1 , 2 ) = 'ij' ;

UPDATE links_frequency.firstname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'c' , 'k' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'c' ;

UPDATE links_frequency.firstname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 's' , 'z' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 's' ;

UPDATE links_frequency.firstname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'i' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'i' ;

UPDATE links_frequency.firstname_a 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'j' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'j' ;


UPDATE links_frequency.firstname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 2 ) , 'ij' , 'y' ) , SUBSTRING( name_2 , 3 ) ) 
WHERE SUBSTRING( name_2 , 1 , 2 ) = 'ij' ;

UPDATE links_frequency.firstname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'c' , 'k' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'c' ;

UPDATE links_frequency.firstname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 's' , 'z' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 's' ;

UPDATE links_frequency.firstname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'i' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'i' ;

UPDATE links_frequency.firstname_b 
SET name_2 = CONCAT( REPLACE( SUBSTRING( name_2 , 1 , 1 ) , 'j' , 'y' ) , SUBSTRING( name_2 , 2 ) ) 
WHERE SUBSTRING( name_2 , 1 , 1 ) = 'j'

