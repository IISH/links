# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		ingest-militieregisters=11.py
Version:	0.1
Goal:		Ingest miltieregister CSV file into MySQL table

15-May-2018	Created
23-May-2018	Changed, different input file from Kees
28-Aug-2018	Changed again, different input file from Kees; 
			Notice: some records have negativeday or month numbers. 

# Action-4, set corresponding id_registration in person_o
UPDATE links_original.registration_o, links_original.person_o 
SET person_o.id_registration = registration_o.id_registration 
WHERE person_o.id_source = 11 
AND registration_o.id_source = 11 
AND person_o.id_person_o = registration_o.id_orig_registration;

# Action-3
-- Copy from temp to original
USE links_temp;
USE links_original;

INSERT INTO links_original.registration_o
(
	id_source,
	name_source,
	id_orig_registration,
	registration_maintype,
	registration_type, 
	registration_day,
	registration_month,
	registration_year
)
SELECT
	Id_source,
	name_source,
	Id_orig_registration,
	Registration_maintype,
	Registration_type,
	Registration_day,
	Registration_month,
	Registration_year
FROM
	links_temp.militieregisters11;

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
	birth_day,
	birth_month,
	birth_year,
	birth_location
)
SELECT
	Id_source,
	Registration_maintype,
	Id_person_o,
	Voornaam,
	Prefix,
	Achternaam,
	Role,
	Geslacht,
	Gebdag,
	Gebmnd,
	Gebjaar,
	Gebplaats
FROM
	links_temp.militieregisters11;

# Action-2
-- Delete previous militie data
USE links_original;
DELETE FROM links_original.registration_o WHERE id_source = 11;
DELETE FROM links_original.person_o WHERE id_source = 11;

# Action-1
# Add a column to table militieregisters, and set the default value: 
USE links_temp;
# Column now already present, but still has to be filled
#ALTER TABLE links_temp.militieregisters11 ADD name_source VARCHAR(100) DEFAULT NULL AFTER id_source;
UPDATE links_temp.militieregisters11 SET name_source = "militieregisters";

# Action-0
# Dump/Restore table schema
$ mysqldump -u links -pmslinks --no-data links_temp militieregisters11 > links_temp.militieregisters11.schema.sql
$ mysql --user=links --password=mslinks links_temp < links_temp.militieregisters11.schema.sql

registration_o
--------------
id_registration			AUTO
id_source				militieregisters11.Id_source
name_source				"militieregisters"
id_orig_registration	militieregisters11.Id_orig_registration
registration_maintype	militieregisters11.Registration_maintype
registration_type		militieregisters11.Registration_type
registration_day		militieregisters11.Registration_day
registration_month		militieregisters11.Registration_month
registration_year		militieregisters11.Registration_year

person_o
--------
id_person				AUTO
id_registration			registration_o.id_registration
id_source				militieregisters11.Id_source
registration_maintype	militieregisters11.Registration_maintype
id_person_o				militieregisters11.Id_person_o
firstname				militieregisters11.Voornaam
familyname				militieregisters11.Achternaam_prefix
role					militieregisters11.Role
sex						militieregisters11.Geslacht
birth_day				militieregisters11.Gebdag
birth_month				militieregisters11.Gebmnd
birth_year				militieregisters11.Gebjaar
birth_location			militieregisters11.location
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
	next, object, oct, open, pow, range, round, super, str, zip )

import chardet		# determine encoding of csv_file
import datetime
import io
import logging
import MySQLdb
import os
import sys
import yaml

#from lxml import etree
from time import time

LIMIT = None

log_file = True

HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""

csv_filename  = "All_Cases_to be matched_naar_LINKS.csv"
csv_separator = ';'
# and assuming "" quoted input strings (not needed for numbers)
id_source = 11

debug = False


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
			logging.info( query )
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
	
	def warnings( self ):
		self.cursor.execute( "SHOW WARNINGS" )
		warnings = self.cursor.fetchall()
		return warnings
	
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


"""
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
"""


def check_encoding( csv_pathname ):
	csv_file = open( csv_pathname, 'rb' )
	rawdata = csv_file.read()
	encoding = chardet.detect( rawdata )
	return encoding



def process_csv( db_links, csv_filename ):
	print( "process_csv()" )
	logging.debug( "process_csv()" )
	logging.info( "read: %s" % csv_filename )
	cur_dir = os.getcwd()
	csv_pathname = os.path.abspath( os.path.join( cur_dir, csv_filename ) )
	logging.info( "read: %s" % csv_pathname )
	encoding_dict = check_encoding( csv_pathname )
	logging.info( "chardet: %s" % encoding_dict )
	encoding = encoding_dict[ "encoding" ]		# ISO-8859-1
	
	csv_header_names = [
		"Id",
		"Id_orig_registration",
		"Id_person_o",
		"Id_source",
		"Registration_maintype",
		"Registration_type",
		"Registration_day",
		"Registration_month",
		"Registration_year",
		"Role",
		"Idnr",
		"Persnr",
		"Relatie_type",
		"Achternaam",
		"Prefix",
		"Voornaam",
		"Geslacht",
		"Gebdag",
		"Gebmnd",
		"Gebjaar",
		"Gebplaats",
		"temp"
	]
	
	map_columns = {
		"Id"                    : "id",						# skip (pk)
		"Id_orig_registration"  : "Id_orig_registration",	# id_orig_registration	r
		"Id_person_o"           : "Id_person_o",			# id_person_o			p
		"Id_source"             : "Id_source",				# id_source				r+p
		"Registration_maintype" : "Registration_maintype",	# registration_maintype	r+p
		"Registration_type"     : "Registration_type",		# registration_type		r
		"Registration_day"      : "Registration_day",		# registration_day		r
		"Registration_month"    : "Registration_month",		# registration_month	r
		"Registration_year"     : "Registration_year",		# registration_year		r
		"Role"                  : "Role",					# role					p
		"Idnr"                  : "Idnr",					# skip
		"Persnr"                : "Persnr",					# skip
		"Relatie_type"          : "Relatie_type",			# skip
		"Achternaam"            : "Achternaam",				# familyname
		"Prefix"                : "Prefix",					# prefix
		"Voornaam"              : "Voornaam",				# firstname				p
		"Geslacht"              : "Geslacht",				# sex					p
		"Gebdag"                : "Gebdag",					# birth_day				p
		"Gebmnd"                : "Gebmnd",					# birth_month			p
		"Gebjaar"               : "Gebjaar",				# birth_year			p
		"Gebplaats"             : "Gebplaats",				# birth_location		p
		"temp"                  : "temp"					# skip
	}
	# added to table            : name_source				# name_source			r
	
	
	nline = 0
	id_person_o_prev = None
	csv_file = io.open( csv_pathname, 'r', encoding = encoding )

	sql = "USE links_temp;"
	print( sql )
	logging.debug( "sql: %s" % sql )
	db_links.query( sql )

	for line in csv_file:
		nline += 1
		#logging.debug( "line %d: %s" % ( nline, line ) )
		line = line.strip( '\n' )		# remove trailing \n
		#logging.debug( "%d in: %s" % ( nline, line ) )
		
		reg_dict = {}

		fields = line.split( csv_separator )
		if nline == 1:
			line_header = line
			logging.debug( line )
			nfields_header = len( fields )		# nfields of header line
			continue	# do not store header line
		else:
			nfields = len( fields )
			if nfields != nfields_header:
				msg = "skipping bad data line # %d" % nline
				logging.debug( msg )
				logging.debug( line )
				continue
			
			for i in range( nfields ):
				csv_header_name = csv_header_names[ i ]
				tbl_header_name = map_columns[ csv_header_name ]
				value = fields[ i ]
				if not value:
					value = '\"\"'
				"""
				else:
					try:
						ival = int( value )
					except ValueError:
						if value == '"':
							value = '\"\"'
						else:
							value = '\"%s\"' % value
				"""
				reg_dict[ tbl_header_name ] = value
				if debug: print( "name: %s, value: %s" % ( tbl_header_name, str( value ) ) )
		
			#if nline > 3:
			#	break
		
		if debug: print( nline )
		
		table = "militieregisters11"
		cols = reg_dict.keys()
		vals = reg_dict.values()
		sql = "INSERT INTO `%s` (%s) VALUES (%s)" % ( table, ",".join( cols ), ",".join( vals ) )
		#print( sql )
		if debug: logging.debug( "sql %d: %s" % ( nline, sql ) )
		affected_count = db_links.insert( sql )
		
		if not affected_count:
			logging.error( "insert failed at line: %d, with sql:" % nline )
			logging.info( "%s" % sql )
		
		warnings = db_links.warnings()
		if warnings:
			logging.info( "sql: %s" % sql )
			lenw = len( warnings )
			print( warnings )
			for w, warning in enumerate( warnings ):
				if w == lenw - 1:
					#logging.warning( "%s" % warnings[ i ][ 2 ] )
					logging.warning( "%s\n" % warning[ 2 ] )
				else:
					logging.warning( "%s" % warning[ 2 ] )
	
	query_cnt = "SELECT COUNT(*) as count FROM %s WHERE id_source = %d" % ( table, id_source )
	print( query_cnt ); logging.info( query_cnt )
	
	resp_cnt = db_links.query( query_cnt )
	dict_cnt = resp_cnt[ 0 ]
	count = dict_cnt[ "count" ]
	msg = "Number of records in table %s: %d" % ( table, count )
	print( msg ); logging.info( msg )



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
		logging_filename = "ingest-militieregisters=11.log"
		logging.basicConfig( filename = logging_filename, filemode = 'w', level = log_level )
	else:
		logging.basicConfig( level = log_level )
	
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	logging.info( msg )
	if log_file:
		print( msg )
		print( "logging to: %s" % logging_filename )
	
	config_path = os.path.join( os.getcwd(), "ingest-militieregisters=11.yaml" )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	DBNAME_LINKS = config.get( "DBNAME_LINKS" )
	
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	
	logging.info( __file__ )
	
	process_csv( db_links, csv_filename )
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	logging.info( msg )
	if log_file: print( msg )
	
	str_elapsed = format_secs( time() - time0 )
	print( "militie_import %s" % str_elapsed )	

# [eof]
