#!/bin/sh

# Project LINKS, KNAW IISH

# FL-28-Dec-2020 Created
# FL-29-Jan-2020 Changed

# Dump LINKS schemas

# We only dump the tables needed for a new installation, and so skip additional tables 
# (dated copies and partial tables) that are present in the current production database. 

# We do not want the AUTO_INCREMENT in the schemas, skip with sed: 
# mysqldump -u root -p -h <db-host> --opt <db-name> -d --single-transaction | sed 's/ AUTO_INCREMENT=[0-9]*//g' > <filename>.sql

# Set usr & pwd here
#usr=...
#pwd=...

# Or get usr & pwd from hsn-links-db.yaml (this needs yq [python] and jq [linux])
yaml=$YAML_MAIN_DIR/hsn-links-db.yaml
usr=`yq --raw-output .USER_LINKS $yaml`
pwd=`yq --raw-output .PASSWD_LINKS $yaml`

filter=" AUTO_INCREMENT=[0-9]*"
today=`date "+%Y.%m.%d"`

# Dump LINKS schemas

# links_a2a db: all 11 tables
db="links_a2a"
tables=""
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# links_original db: only tables person_o and registration_o
db="links_original"
tables="person_o registration_o"
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# links_cleaned db: only tables person_c and registration_c
db="links_cleaned"
tables="person_c registration_c"
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# links_logs db: only ERROR_STORE table
db="links_logs"
tables="ERROR_STORE"
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# links_prematch db: 12 tables
db="links_prematch"
tables="freq_familyname freq_firstname freq_firstname_sex links_base ls_familyname ls_familyname_first ls_familyname_strict ls_firstname ls_firstname_first ls_firstname_strict root_familyname root_firstname"
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# links_match db: 6 tables
db="links_match"
tables="MATCHES_CBG_WWW match_process match_view matches matrix notation"
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# links_root db: all 10 tables
# mixed MyISAM / InnoDB ?
db="links_root"
tables=""
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# links_ids db: all 7 tables
# mixed MyISAM / InnoDB ?
db="links_ids"
tables=""
params="-u $usr -p$pwd --no-data $db  $tables -d --single-transaction"
mysqldump $params | sed "s/${filter}//g" > $db.schema-$today.sql

# [eof]
