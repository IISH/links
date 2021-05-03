#!/bin/sh

# Project LINKS, KNAW IISH

# FL-28-Dec-2020 Created
# FL-03-May-2021 Changed

# Restore LINKS schemas in EMPTY databases

# set the date to select the (probably latest) dumps to restore
#someday="2021.05.03"

# Set usr & pwd here
#usr=...
#pwd=...

# Or get usr & pwd from hsn-links-db.yaml (this needs yq [python] and jq [linux])
# activate the virtual python
PYTHON3_HOME=$LINKS_HOME/python392
source $PYTHON3_HOME/bin/activate
yaml=$LINKS_HOME/python/hsn-links-db.yaml
usr=`yq --raw-output .USER_LINKS $yaml`
pwd=`yq --raw-output .PASSWD_LINKS $yaml`

mysql --user=$usr --password=$pwd links_a2a      < $someday/links_a2a.schema-$someday.sql
mysql --user=$usr --password=$pwd links_original < $someday/links_original.schema-$someday.sql
mysql --user=$usr --password=$pwd links_cleaned  < $someday/links_cleaned.schema-$someday.sql
mysql --user=$usr --password=$pwd links_logs     < $someday/links_logs.schema-$someday.sql
mysql --user=$usr --password=$pwd links_prematch < $someday/links_prematch.schema-$someday.sql
mysql --user=$usr --password=$pwd links_match    < $someday/links_match.schema-$someday.sql
mysql --user=$usr --password=$pwd links_root     < $someday/links_root.schema-$someday.sql
mysql --user=$usr --password=$pwd links_ids      < $someday/links_ids.schema-$someday.sql

#[eof]
