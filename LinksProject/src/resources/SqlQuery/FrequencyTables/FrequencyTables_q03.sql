-- query 03
INSERT INTO links_frequency.firstfirstname( name , frequency )
    SELECT firstname1, COUNT(*) AS frequency 
    FROM links_cleaned.person_c 
    GROUP BY firstname1 ;
