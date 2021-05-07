-- Created by Omar Azouguagh
-- Edited by Fons Laan
-- 26-Aug-2014 registration_maintype added (from links_a2a.source collection)
-- 02-Oct-2014 location -> living_location in person_o
-- 28-Oct-2014 copy all 5 age fields from a2a
-- 04-Nov-2014 change RelationPP copying
-- 04-Dec-2014 gender -> SUBSTRING( gender, 1, 1 )
-- 06-Jan-2015 notice: 1 query below uses the stored function links_functions.regex_replace
-- 13-Jan-2015 titte_other -> title_other
-- 01-Apr-2015 missing mar_location, divorce_location, death_location
-- 14-Apr-2015 remove id_registratie_spec
-- 16-Sep-2015 registration_o_temp, add: registration_day, registration_month, registration_year
-- 20-Feb-2016 new name: a2a_to_original.sql
-- 11-Apr-2016 show registration counts from source
-- 23-May-2016 also copy source.institutionname
-- 05-Jun-2018 no longer use MysQL FUNCTION: links_functions.regex_replace
-- 05-Jun-2018 add role = 'other:Gewezen echtgenoot' & 'other:Gewezen echtgenote' for 'Echtscheiding'
-- 11-Jun-2018 copy divorce_date from person_o_temp
-- 24-Jan-2021 patronyme => patronym
-- 30-Apr-2021 also copy A2A source.sourcedigitaloriginal

TRUNCATE TABLE links_a2a.registration_o_temp ;

--

TRUNCATE TABLE links_a2a.person_o_temp ;

--

INSERT INTO 
	links_a2a.registration_o_temp
	(
		id_source,
		name_source,
		id_orig_registration,
		id_persist_registration,
		source_digital_original,
		registration_maintype,
		registration_location,
		registration_date,
		registration_day,
		registration_month,
		registration_year,
		registration_seq
	)
SELECT
	archive,
	institutionname,
	a2a_id,
	recordguid,
	sourcedigitaloriginal,
	collection,
	sourceplace_place,
	literaldate,
	day,
	month,
	year,
	documentnumber
FROM 
	links_a2a.source ;

--

UPDATE 
	links_a2a.registration_o_temp AS a , 
	links_a2a.event AS b
SET
	a.registration_type = b.eventtype ,
	a.registration_church = b.religionliteral
WHERE 
	a.id_orig_registration = b.a2a_id ;

--

UPDATE 
	links_a2a.registration_o_temp AS a ,
	links_a2a.remark AS b
SET
	a.remarks = b.value 
WHERE
	a.id_orig_registration = b.a2a_id AND 
	b.type = 'source_sourceremark' AND 
	b.remark_key = 'Opmerking';

--

INSERT INTO links_a2a.person_o_temp
	(
		id_registration,
		id_person_o,
		title_noble,
		title_other,
		firstname,
		initials,
		patronym,
		prefix,
		familyname,
		alias_familyname,
		sex,
		location,
		birth_location,
		religion,
		civil_status,
		age_literal,
		age_year,
		age_month,
		age_week,
		age_day
	)
SELECT
	a2a_id,
	pid,
	personnametitleofnobility,
	personnametitle,
	personnamefirstname,
	personnameinitials,
	personnamepatronym,
	personnameprefixlastname,
	personnamelastname,
	personnamealias,
	SUBSTRING( gender, 1, 1 ),
	residence_place,
	birthplace_place,
	personreligionliteral,
	maritalstatus,
	personageliteral,
	personageyears,
	personagemonths,
	personageweeks,
	personagedays
FROM
	links_a2a.person;

--

UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.relation AS b
SET
	a.role = b.relationtype
WHERE
	a.id_person_o = b.keyref_1 AND 
	a.id_registration = b.a2a_id AND
	b.relation = 'RelationEP' ;
	
--

-- Notice: a.id_person_o is never equal to b.keyref_2
-- Notice 'RelationEP' must be executed before 'RelationPP'
UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.relation AS b
SET
	a.role = b.relationtype
WHERE
	a.role IS NULL AND 
	a.id_registration = b.a2a_id AND
	b.relation = 'RelationPP' ;

--

UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.event AS b , 
	links_a2a.registration_o_temp AS c
SET
	a.birth_date = CONCAT( day, '-', month, '-', year )
WHERE
	a.role = 'Kind' AND
	c.registration_type = 'Geboorte' AND
	a.id_registration = c.id_orig_registration AND
	c.id_orig_registration = b.a2a_id ;

--

UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.event AS b , 
	links_a2a.registration_o_temp AS c
SET
	a.mar_date = CONCAT( day, '-', month, '-', year ) , 
	a.mar_location = b.place 
WHERE
	( a.role = 'Bruid' OR a.role = 'Bruidegom' ) AND
	c.registration_type = 'Huwelijk' AND
	a.id_registration = c.id_orig_registration AND
	c.id_orig_registration = b.a2a_id ;

--

UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.event AS b , 
	links_a2a.registration_o_temp AS c
SET
	a.divorce_date = CONCAT( day, '-', month, '-', year ) , 
	a.divorce_location = b.place 
WHERE
	( a.role = 'Bruid' OR a.role = 'Bruidegom' OR a.role = 'other:Gewezen echtgenoot' OR a.role = 'other:Gewezen echtgenote' ) AND
	c.registration_type = 'Echtscheiding' AND
	a.id_registration = c.id_orig_registration AND
	c.id_orig_registration = b.a2a_id ;

--
	
UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.event AS b , 
	links_a2a.registration_o_temp AS c
SET
	a.death_date = CONCAT( day, '-', month, '-', year ) , 
	a.death_location = b.place
WHERE
	a.role = 'Overledene' AND
	c.registration_type = 'Overlijden' AND
	a.id_registration = c.id_orig_registration AND
	c.id_orig_registration = b.a2a_id ;

--

UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.person_profession AS b
SET 
	a.occupation = b.content
WHERE 
	a.id_person_o = b.pid and b.a2a_id=a.id_registration;
	
-- 

SELECT 
     @m := IF(max( id_registration ) is NULL , 0, max( id_registration ) ) 
FROM 
    links_original.registration_o;
    
--

UPDATE 
	links_a2a.registration_o_temp AS a
SET 
	a.id_registration = @m + a.id_orig_registration ;

--

UPDATE 
	links_a2a.person_o_temp AS a , 
	links_a2a.registration_o_temp AS b
SET 
	a.id_registration = b.id_registration
WHERE
	a.id_registration = b.id_orig_registration ;

--

-- UPDATE 
-- 	links_a2a.person_o_temp AS a
-- SET 
-- 	a.id_person_o = links_functions.regex_replace(a.id_person_o, '[^0-9]+', '' ) ;
--

UPDATE 
	links_a2a.person_o_temp AS p , 
	links_a2a.registration_o_temp AS r
SET 
	p.id_source = r.id_source
WHERE 
	p.id_registration = r.id_registration ;

--
-- FL-27-Aug-2014 copy registration_maintype from registration_o_temp to person_o_temp
UPDATE
	links_a2a.person_o_temp AS p , 
	links_a2a.registration_o_temp AS r
SET 
	p.registration_maintype = r.registration_maintype
WHERE 
	p.id_registration = r.id_registration ;

--

INSERT INTO links_original.registration_o
	(
		id_registration,
		id_source,
		name_source,
		id_persist_source,
		id_persist_registration,
		source_digital_original,
		id_orig_registration,
		registration_maintype,
		registration_type,
		extract,
		registration_location,
		registration_church,
		registration_date,
		registration_day,
		registration_month,
		registration_year,
		registration_seq,
		remarks
	)
SELECT
	id_registration,
	id_source,
	name_source,
	id_persist_source,
	id_persist_registration,
	source_digital_original,
	id_orig_registration,
	registration_maintype,
	registration_type,
	extract,
	registration_location,
	registration_church,
	registration_date,
	registration_day,
	registration_month,
	registration_year,
	registration_seq,
	remarks
FROM
	links_a2a.registration_o_temp ;
	
--

INSERT INTO links_original.person_o
	(
		id_registration,
		id_source,
		registration_maintype,
		id_person_o,
		title_noble,
		title_other,
		firstname,
		alias_firstname,
		initials,
		patronym,
		prefix,
		familyname,
		alias_familyname,
		suffix,
		sex,
		religion,
		civil_status,
		role,
		occupation,
		living_location,
		birth_location,
		mar_location,
		divorce_location,
		death_location,
		age_literal,
		age_year,
		age_month,
		age_week,
		age_day,
		birth_date,
		mar_date,
		divorce_date,
		death_date
	)
SELECT
	id_registration,
	id_source,
	registration_maintype,
	id_person_o,
	title_noble,
	title_other,
	firstname,
	alias_firstname,
	initials,
	patronym,
	prefix,
	familyname,
	alias_familyname,
	suffix,
	sex,
	religion,
	civil_status,
	role,
	occupation,
	location,
	birth_location,
	mar_location,
	divorce_location,
	death_location,
	age_literal,
	age_year,
	age_month,
	age_week,
	age_day,
	birth_date,
	mar_date,
	divorce_date,
	death_date
FROM
	links_a2a.person_o_temp ;

--
-- FL-11-Apr-2016
SELECT archive, institutionname, COUNT(*) AS count FROM links_a2a.source GROUP BY archive, institutionname;
