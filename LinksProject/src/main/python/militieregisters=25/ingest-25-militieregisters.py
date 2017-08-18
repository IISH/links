# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		ingest-25-militieregisters.py
Version:	0.2
Goal:		Ingest miltieregister XML files into MySQL table

18-Apr-2017	Created
28-Jun-2017	Move empty string initialization inside loop in process_xml()
18-Aug-2017	Latest change


-- SQL manipulations afterwards (only once):
USE links_original;
ALTER TABLE `registration_o` ADD INDEX `id_source` (`id_source`);
ALTER TABLE `person_o` ADD INDEX `id_source` (`id_source`);

USE links_original;
ALTER TABLE `registration_o` ADD INDEX `id_orig_registration` (`id_orig_registration`);
USE links_original;
ALTER TABLE `person_o` ADD INDEX `id_person_o` (`id_person_o`);


-- Delete previous data
USE links_original;
DELETE FROM links_original.registration_o WHERE id_source = 25;
DELETE FROM links_original.person_o WHERE id_source = 25;

-- Copy from temp
USE links_temp;
USE links_original;

INSERT INTO links_original.registration_o
(
	id_source,
	name_source,
	id_orig_registration,
	registration_maintype,
	registration_date
)
SELECT
	id_source,
	name_source,
	id,
	registration_maintype,
	date
FROM
	links_temp.militieregisters;

INSERT INTO links_original.person_o
(
	id_source,
	registration_maintype,
	id_person_o,
	firstname,
	prefix,
	familyname,
	role,
	sex,
	birth_date,
	birth_location
)
SELECT
	id_source,
	registration_maintype,
	id,
	given,
	prefix,
	surname,
	role,
	sex,
	date,
	location
FROM
	links_temp.militieregisters;


UPDATE links_original.registration_o, links_original.person_o 
SET person_o.id_registration = registration_o.id_registration 
WHERE person_o.id_source = 25 
AND registration_o.id_source = 25 
AND person_o.id_person_o = registration_o.id_orig_registration;


# Add some columns to table militieregisters, and set default values: 
USE links_temp;

ALTER TABLE links_temp.militieregisters ADD id_source INT(10) UNSIGNED DEFAULT NULL AFTER id;
ALTER TABLE links_temp.militieregisters ADD name_source VARCHAR(100) DEFAULT NULL AFTER id_source;
ALTER TABLE links_temp.militieregisters ADD registration_maintype TINYINT(3) UNSIGNED DEFAULT NULL AFTER name_source;
ALTER TABLE links_temp.militieregisters ADD sex CHAR(1) DEFAULT NULL AFTER registration_maintype;
ALTER TABLE links_temp.militieregisters ADD role VARCHAR(50) DEFAULT NULL AFTER sex;

UPDATE links_temp.militieregisters SET id_source = 25;
UPDATE links_temp.militieregisters SET name_source = "militieregisters";
UPDATE links_temp.militieregisters SET registration_maintype = 1;
UPDATE links_temp.militieregisters SET sex = "m";
UPDATE links_temp.militieregisters SET role = "Kind";


registration_o
--------------
id_registration			AUTO
id_source				25
name_source				"militieregisters"
id_orig_registration	militieregisters.id
registration_maintype	1
registration_date		militieregisters.date	# reverse date string! OK

person_o
--------
id_person				AUTO
id_registration			registration_o.id_registration
id_source				25
registration_maintype	1
id_person_o				militieregisters.id
firstname				militieregisters.given
prefix					militieregisters.prefix
familyname				militieregisters.surname
role					1
sex						"m"
birth_date				militieregisters.date	# reverse date string! OK
birth_location			militieregisters.location
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
	next, object, oct, open, pow, range, round, super, str, zip )

import datetime
import logging
import MySQLdb
import os
import sys
import yaml

from lxml import etree
from time import time

LIMIT = None

log_file = True

HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""


class Database:
	def __init__( self, host, user, passwd, dbname ):
	#	print( "host:   %s" % host )
	#	print( "user:   %s" % user )
	#	print( "passwd: %s" % passwd )
	#	print( "dbname: %s" % dbname )
		
		self.host   = host
		self.user   = user
		self.passwd = passwd
		self.dbname = dbname

		self.connection = MySQLdb.connect( \
			host = self.host, 
			user = self.user, 
			passwd = self.passwd, 
			db = self.dbname,
			charset     = "utf8",			# needed when there is e.g. 
			use_unicode = True				# &...; html escape stuff in strings
		)
		self.cursor = self.connection.cursor()

	def insert( self, query ):
		affected_count = None
		try:
			affected_count = self.cursor.execute( query )
			self.connection.commit()
		except:
			self.connection.rollback()
			etype = sys.exc_info()[ 0:1 ]
			value = sys.exc_info()[ 1:2 ]
			logging.error( "%s, %s\n" % ( etype, value ) )
		return affected_count
	
	def update( self, query ):
		return self.insert( query )
	
	def query( self, query ):
		logging.debug( "\n%s" % query )
		cursor = self.connection.cursor( MySQLdb.cursors.DictCursor )
		cursor.execute( query )
		return cursor.fetchall()

	def info( self ):
		"""
		See the MySQLdb User's Guide: 
		Returns some information about the last query. Normally you don't need to check this. If there are any MySQL 
		warnings, it will cause a Warning to be issued through the Python warning module. By default, Warning causes 
		a message to appear on the console. However, it is possible to filter these out or cause Warning to be raised 
		as exception. See the MySQL docs for mysql_info(), and the Python warning module. (Non-standard)
		"""
		return self.connection.info()

	def __del__( self ):
		self.connection.close()



def escape( s ):
	s_esc = ""
	
	if s is None:
		s_esc = ""
	else:
		s_esc = s
		s_esc = s_esc.replace( '\\', '\\\\' )
		s_esc = s_esc.replace( "'",  "\\'" )
		s_esc = s_esc.replace( '"',  '\\"' )
	
	return s_esc



def store_record( record_id, file_id, coords, given, prefix, surname, ev_type, location, date ): 
	given_esc    = escape( given )
	prefix_esc   = escape( prefix )
	surname_esc  = escape( surname )
	location_esc = escape( location )
	
	line = " \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\" " % ( record_id, file_id, coords, given_esc, prefix_esc, surname_esc, ev_type, location_esc, date )
	logging.debug( line )
	
	query  = ""
	query += "INSERT INTO links_temp.militieregisters "
	query += "( record_id, file_id, coords, given, prefix, surname, ev_type, location, date ) "
	query += "VALUES ( " 
	query += line
	query += " );"
	
	logging.debug( query )
	
	resp = db_links.insert( query )
	logging.debug( resp )



def process_xml( db_links, xml_path ):
	logging.debug( "process_xml()" )
	logging.info( "use: %s" % xml_path )

	NM = "{http://bit.pub/files}"
	len_nm = len( NM )
	
	# both events used for section names, for others only start is used
	events = ( "start", "end" )

	record_id = ""
	file_id   = ""
	coords    = ""
	given     = ""
	prefix    = ""
	surname   = ""
	ev_type   = ""
	location  = ""
	date      = ""

	empty_vars = set()		# which vars were not set via the xml?

	chunk   = 1000
	nrecord = 0
	nevent  = 0
	for event, elem in etree.iterparse( xml_path, events ):
		if LIMIT is not None:
			if nrecord > LIMIT:
				break
		
		tag_nm = elem.tag
		tag = tag_nm[ len_nm: ]
	#	logging.debug( "tag_nm: %s, event: %s" % ( tag_nm, event ) )
		
		if event == "start":		# create new section object
			if tag == "record":
				if nrecord > 0 and ( nrecord + chunk ) % chunk == 0:
					print( "processed # of records: %d" % nrecord )
				
				logging.debug( "--- record # %d ---" % nrecord )
				nrecord += 1
				record_id = elem.get( "id" )
				logging.debug( "record_id: %s" % record_id )
			
			if tag == "file":
				file_id = elem.get( "id" )
				logging.debug( "file_id: %s" % file_id )
				coords = elem.get( "coords" )
				logging.debug( "coords: %s" % coords )
			
			if tag == "person":
				#person_id = elem.get( "id" )
				#logging.debug( "person_id: %s" % person_id )
				pass
			
			if tag == "given":
				given = elem.text
				logging.debug( "given: %s" % given )
			
			if tag == "prefix":
				prefix = elem.text
				logging.debug( "prefix: %s" % prefix )
			
			if tag == "surname":
				surname = elem.text
				logging.debug( "surname: %s" % surname )
			
			if tag == "place":
				#place_id = elem.get( "id" )
				#logging.debug( "place_id: %s" % place_id )
				ev_type = elem.get( "event" )
				logging.debug( "ev_type: %s" % ev_type )
			
			if tag == "name":
				location = elem.text
				logging.debug( "location: %s" % location )
			
			#if tag == "link":
			#	link_place = elem.text
			#	logging.debug( "link_place: %s" % link_place )
			
			if tag == "date":
				#date_id = elem.get( "id" )
				#logging.debug( "date_id: %s" % date_id )
				ev_type = elem.get( "event" )
				logging.debug( "ev_type: %s" % ev_type )
			
			if tag == "ymd":
				date_yyyymmdd = elem.text		# reverse components
				if date_yyyymmdd is None:
					date = ""
				else:
					date_comps = date_yyyymmdd.split( '-' )
					date = date_comps[ 2 ] + '-' + date_comps[ 1 ] + '-' + date_comps[ 0 ]
					logging.debug( "date: %s" % date )
			
			#if tag == "link":
			#	link_date = elem.text
			#	logging.debug( "link_date: %s" % link_date )
		
		elif event == "end":
			if tag == "record":
				store_record( record_id, file_id, coords, given, prefix, surname, ev_type, location, date )
				
				# check None or empty vars
				if not record_id: empty_vars.add( "record_id" )
				if not file_id:   empty_vars.add( "file_id" )
				if not coords:    empty_vars.add( "coords" )
				if not given:     empty_vars.add( "given" )
				if not prefix:    empty_vars.add( "prefix" )
				if not surname:   empty_vars.add( "surname" )
				if not ev_type:   empty_vars.add( "ev_type" )
				if not location:  empty_vars.add( "location" )
				if not date:      empty_vars.add( "date" )
				
				# clear vars for new record
				record_id = ""
				file_id   = ""
				coords    = ""
				given     = ""
				prefix    = ""
				surname   = ""
				ev_type   = ""
				location  = ""
				date      = ""

	query_cnt  = "SELECT COUNT(*) as count FROM links_temp.militieregisters;"
	logging.info( query_cnt )
	
	resp_cnt = db_links.query( query_cnt )
	dict_cnt = resp_cnt[ 0 ]
	count = dict_cnt[ "count" ]
	msg = "Number of records in militieregisters: %d" % count
	print( msg ); logging.info( msg )

	msg = "empty vars in some records: %s" % empty_vars
	print( msg ); logging.info( msg )



def process_batches( db_links ):
	logging.debug( "process_batches()" )
	cur_dir = os.getcwd()
	batch_dir  = os.path.join( cur_dir, "batches" )
	dir_list = []
	if os.path.isdir( batch_dir ):
		dir_list = os.listdir( batch_dir )
		dir_list.sort()
		msg = "using batch directory: %s" % batch_dir
		print( msg ); logging.info( msg )

	query_t  = "TRUNCATE TABLE links_temp.militieregisters;"
	logging.info( query_t )
	resp_t = db_links.query( query_t )
	if len( resp_t ) != 0:
		print( resp_t ); logging.info( resp_t )

	for filename in dir_list:
		print( filename )
		root, ext = os.path.splitext( filename )
		if ext == ".xml":
			logging.info( "read: %s" % filename )
			xml_pathname = os.path.abspath( os.path.join( batch_dir, filename ) )
			process_xml( db_links, xml_pathname )
			
			#break		# 1 file test
		else:
			logging.debug( "skip: %s" % filename )



def format_secs( seconds ):
	nmin, nsec  = divmod( seconds, 60 )
	nhour, nmin = divmod( nmin, 60 )

	if nhour > 0:
		str_elapsed = "%d:%02d:%02d (hh:mm:ss)" % ( nhour, nmin, nsec )
	else:
		if nmin > 0:
			str_elapsed = "%02d:%02d (mm:ss)" % ( nmin, nsec )
		else:
			str_elapsed = "%d (sec)" % nsec

	return str_elapsed



if __name__ == "__main__":
	log_level = logging.DEBUG
	#log_level = logging.INFO
	#log_level = logging.WARNING
	#log_level = logging.ERROR
	#log_level = logging.CRITICAL

	if log_file:
		logging_filename = "ingest-25-militieregisters.log"
		logging.basicConfig( filename = logging_filename, filemode = 'w', level = log_level )
	else:
		logging.basicConfig( level = log_level )
	
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	logging.info( msg )
	if log_file:
		print( msg )
		print( "logging to: %s" % logging_filename )
	
	HOST_LINKS   = "localhost"
	USER_LINKS   = "links"
	PASSWD_LINKS = "mslinks"
	
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	
	logging.info( __file__ )
	
	process_batches( db_links )
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	logging.info( msg )
	if log_file: print( msg )
	
	str_elapsed = format_secs( time() - time0 )
	print( "militie_import %s" % str_elapsed )	

# [eof]
