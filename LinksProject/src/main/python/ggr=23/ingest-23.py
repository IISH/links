# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		ingest-23.py
Version:	0.4
Goal:		Ingest id_source = 23

USE links_temp;
TRUNCATE TABLE links_temp.ggr_r;
TRUNCATE TABLE links_temp.ggr_p;

USE links_original;
DELETE FROM links_original.registration_o WHERE id_source = 23;
DELETE FROM links_original.person_o WHERE id_source = 23;

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
	id_source,
	name_source,
	id_orig_registration,
	registration_maintype,
	registration_type,
	registration_day,
	registration_month,
	registration_year
FROM
	links_temp.ggr_r;

-- Copy ggr.id_orig_registration to person_o.id_person_o (NOT ggr.id_person_o)
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
	birth_date
)
SELECT
	id_source,
	registration_maintype,
	id_orig_registration,
	firstname,
	prefix,
	familyname,
	role,
	sex,
	birth_day,
	birth_month,
	birth_year,
	birth_date
FROM
	links_temp.ggr_p;

-- We use id_orig_registration to link the two tables
UPDATE links_original.registration_o, links_original.person_o 
SET person_o.id_registration = registration_o.id_registration 
WHERE person_o.id_source = 23 
AND registration_o.id_source = 23 
AND person_o.id_person_o = registration_o.id_orig_registration;

21-Jul-2017 Created
25-Aug-2017 Latest change
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

from time import time

csv_filename_r = "data/Gen_0_1_2_voor_links_export_registrations_2.csv"
csv_filename_p = "data/Gen_0_1_2_voor_links_export_3.csv"

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



def check_encoding( csv_pathname ):
	csv_file = open( csv_pathname, 'rb' )
	rawdata = csv_file.read()
	encoding = chardet.detect( rawdata )
	return encoding



def process_csv_r( db_links, csv_filename ):
	print( "process_csv_r()" )
	logging.debug( "process_csv_r()" )
	logging.info( "read: %s" % csv_filename )
	cur_dir = os.getcwd()
	csv_pathname = os.path.abspath( os.path.join( cur_dir, csv_filename ) )
	logging.info( "read: %s" % csv_pathname )
	encoding_dict = check_encoding( csv_pathname )
	logging.info( "chardet: %s" % encoding_dict )
	encoding = encoding_dict[ "encoding" ]		# ISO-8859-1
	
	csv_header_names = [
		"REGISTRATIE_NR",
		"REGISTRATION_MAINTYPE",
		"ID_SOURCE",
		"reg_day",
		"reg_month",
		"reg_year"
	]
	
	map_columns = {
		"REGISTRATIE_NR"        : "id_orig_registration",
		"REGISTRATION_MAINTYPE" : "registration_maintype",
		"ID_SOURCE"             : "id_source",
		"reg_day"               : "registration_day",
		"reg_month"             : "registration_month",
		"reg_year"              : "registration_year"
	}

	nline = 0
	id_person_o_prev = None
	csv_file = io.open( csv_pathname, 'r', encoding = encoding )

	for line in csv_file:
		nline += 1
		#logging.debug( "line %d: %s" % ( nline, line ) )
		line = line.strip( '\n' )		# remove trailing \n
		logging.debug( "%d in: %s" % ( nline, line ) )
		
		reg_dict = { 
			"name_source" : '\"ggr\"', 
			"registration_type" : '\"Huwelijk\"'
		}

		fields = line.split( ';' )
		if nline == 1:
			line_header = line
			nfields_header = len( fields )		# nfields of header line
			continue	# do not store header line
		else:
			nfields = len( fields )
			if nfields != nfields_header:
				msg = "skipping bad data line # %d" % nline
				continue
			
			for i in range( nfields ):
				csv_header_name = csv_header_names[ i ]
				tbl_header_name = map_columns[ csv_header_name ]
				value = fields[ i ]
				
				reg_dict[ tbl_header_name ] = value
	
		#print( nline )
		
		table = "links_temp.ggr_r"
		cols = reg_dict.keys()
		vals = reg_dict.values()
		sql_r = "INSERT INTO %s (%s) VALUES(%s)" % ( table, ",".join( cols ), ",".join( vals ) )
		#print( sql_r )
		logging.debug( "sql_r: %s" % sql_r )
		db_links.insert( sql_r )

	id_source = 23
	query_cnt = "SELECT COUNT(*) as count FROM %s WHERE id_source = %d" % ( table, id_source )
	print( query_cnt ); logging.info( query_cnt )
	
	resp_cnt = db_links.query( query_cnt )
	dict_cnt = resp_cnt[ 0 ]
	count = dict_cnt[ "count" ]
	msg = "Number of records in table %s: %d" % ( table, count )
	print( msg ); logging.info( msg )



def process_csv_p( db_links, csv_filename ):
	print( "process_csv_p()" )
	logging.debug( "process_csv_p()" )
	logging.info( "read: %s" % csv_filename )
	cur_dir = os.getcwd()
	csv_pathname = os.path.abspath( os.path.join( cur_dir, csv_filename ) )
	encoding_dict = check_encoding( csv_pathname )
	logging.info( "chardet: %s" % encoding_dict )
	encoding = encoding_dict[ "encoding" ]		# ISO-8859-1
	
	csv_header_names = [
		"REGISTRATIE_NR",
		"Id_person_o",
		"REGISTRATION_MAINTYPE",
		"ID_SOURCE",
		"birth_day",
		"birth_month",
		"birth_year",
		"birth_date",
		"role_links",
		"firstname",
		"familyname",
		"prefix",
		"sex"
	]
	
	map_columns = {
		"REGISTRATIE_NR"        : "id_orig_registration",
		"Id_person_o"           : "id_person_o",
		"REGISTRATION_MAINTYPE" : "registration_maintype",
		"ID_SOURCE"             : "id_source",
		"birth_day"             : "birth_day",
		"birth_month"           : "birth_month",
		"birth_year"            : "birth_year",
		"birth_date"            : "birth_date",
		"role_links"            : "role",
		"firstname"             : "firstname",
		"familyname"            : "familyname",
		"prefix"                : "prefix",
		"sex"                   : "sex"
	}
	
	nline = 0
	id_person_o_prev = None
	csv_file = io.open( csv_pathname, 'r', encoding = encoding )
	
	for line in csv_file:
		nline += 1
		#logging.debug( "line %d: %s" % ( nline, line ) )
		line = line.strip( '\n' )		# remove trailing \n
		logging.debug( "%d in: %s" % ( nline, line ) )
		
		wrong_date_comps = False
		out_dict = { 
			"name_source" : '\"ggr\"', 
			"registration_type" : '\"Huwelijk\"' 
		}
		
		fields = line.split( ';' )
		if nline == 1:
			line_header = line
			nfields_header = len( fields )		# nfields of header line
			continue	# do not store header line
		else:
			nfields = len( fields )
			if nfields != nfields_header:
				msg = "skipping bad data line # %d" % nline
				continue
			
			for i in range( nfields ):
				csv_header_name = csv_header_names[ i ]
				tbl_header_name = map_columns[ csv_header_name ]
				value = fields[ i ]
				
				if tbl_header_name in [ "birth_date", "role", "firstname", "familyname", "prefix", "sex" ]:
					out_dict[ tbl_header_name ] = '\"' + value + '\"'
				else:
					if int( value ) <= 0:
						wrong_date_comps = True
						#print( "nline: %d, csv_header_name: %s, value: %s" % (nline, csv_header_name, value ) )
					out_dict[ tbl_header_name ] = value

		if wrong_date_comps:
			# input cvs has split the date incorrectly, re-establish the components
			date  = out_dict[ "birth_date" ]
			
			in_day   = out_dict[ "birth_day" ]
			in_month = out_dict[ "birth_month" ]
			in_year  = out_dict[ "birth_year" ]
			
			date_comps = date.split( '-' )
			
			out_day    = date_comps[ 0 ]
			out_month  = date_comps[ 1 ]
			out_year   = date_comps[ 2 ]
			
			out_dict[ "birth_day" ]   = out_day
			out_dict[ "birth_month" ] = out_month
			out_dict[ "birth_year" ]  = out_year
			
			msg = "nline: %d, date: %s, in_day: %s, in_month: %s, in_year: %s, => out_day: %s, out_month: %s, out_year: %s" % \
				( nline, date, in_day, in_month, in_year, out_day, out_month, out_year )
			logging.debug( msg )
			print( msg )
	
		#print( nline )
	
		table = "links_temp.ggr_p"
		cols = out_dict.keys()
		vals = out_dict.values()
		sql = "INSERT INTO %s (%s) VALUES(%s)" % ( table, ",".join( cols ), ",".join( vals ) )
		#print( sql )
		logging.debug( "sql: %s" % sql )
		db_links.insert( sql )

	id_source = 23
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
	print( __file__ )
	#log_level = logging.DEBUG
	log_level = logging.INFO
	#log_level = logging.WARNING
	#log_level = logging.ERROR
	#log_level = logging.CRITICAL

	if log_file:
		logging_filename = "ingest-23.log"
		logging.basicConfig( filename = logging_filename, filemode = 'w', level = log_level )
	else:
		logging.basicConfig( level = log_level )
	
	time0 = time()	  # seconds since the epoch
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
	
	process_csv_r( db_links, csv_filename_r )
	process_csv_p( db_links, csv_filename_p )
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	logging.info( msg )
	if log_file: print( msg )
	
	str_elapsed = format_secs( time() - time0 )
	print( "ingest-23 %s" % str_elapsed )  

# [eof]
