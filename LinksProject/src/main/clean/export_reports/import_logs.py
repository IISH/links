#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		import_logs.py
Version:	0.1
Goal:		collect error log tables into a single table ERROR_STORE
Notice:		See the variable x_codes below. If the ref_report table is updated 
			with new report_type values for 'x' codes, this variable must also 
			be updated. 

05-Sep-2016 Created
17-Dec-2018 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import datetime
import MySQLdb
import os
import sys
import yaml

from dateutil.parser import parse
from time import time

debug = False
chunk = 100000		# show progress in processing records

#begin_date_default = "2016-04-15"
#end_date_default   = "2016-05-12"
begin_date_default = "2016-09-08"
end_date_default   = "2016-09-09"

# settings, read from config file
HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""

HOST_REF   = ""
USER_REF   = ""
PASSWD_REF = ""
DBNAME_REF = ""

single_quote = "'"
double_quote = '"'

# column 'type' of ref_report, where column 'content' contains 'standard_code= 'x''.
x_codes = [ 21, 31, 41, 51, 61, 71, 81, 91, 141, 251, 1009, 1109 ]

	
class Database:
	def __init__( self, host, user, passwd, dbname ):
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
		try:
			resp = self.cursor.execute( query )
			self.connection.commit()
		except:
			self.connection.rollback()
			etype = sys.exc_info()[ 0:1 ]
			value = sys.exc_info()[ 1:2 ]
			print( "%s, %s\n" % ( etype, value ) )

	def query( self, query ):
	#	print( "\n%s" % query )
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



def db_check( db ):
	if debug: print( "db_check()" )

	# links_a2a
	#tables = [ "a2a", "event", "object", "person", "person_o_temp", "person_profession", 
	#	"registration_o_temp", "relation", "remark", "source", "source_sourceavailablescans_scan" ]
	
	db_name = "links_match"
	tables = [ "match_process", "match_view", "matches", "matrix", "notation" ]

	print( "table row counts:" )
	for table in tables:
		query = """SELECT COUNT(*) FROM %s.%s""" % ( db_name, table )
		resp = db.query( query )
		if resp is not None:
			count_dict = resp[ 0 ]
			count = count_dict[ "COUNT(*)" ]
			print( "%s %d" % ( table, count ) )
		else:
			print( "Null response from db" )

	# we could show the strings from these GROUPs for cheking, because there should be no variation
	# SELECT eventtype, COUNT(*) FROM links_a2a.event GROUP BY eventtype;
	# SELECT relationtype, COUNT(*) FROM links_a2a.relation GROUP BY relationtype;
	# SELECT relation, COUNT(*) FROM links_a2a.relation GROUP BY relation;
	# SELECT remark_key, COUNT(*) FROM links_a2a.remark GROUP BY remark_key;



def get_log_names( db_logs ):
	if debug: print( "get_log_names()" )
	
	log_names = []
	
	query = "USE links_logs;"
	#print( query )
	resp = db_logs.query( query )
	if resp is None:
		print( "Null response from db_logs" )
		return log_names

	query = "SHOW TABLES;"
	#print( query )
	resp = db_logs.query( query )
	if resp is not None:
		ntables = len( resp )
		for t in range( ntables ):
			resp_t = resp[ t ]
			#print( resp[ t ] )
			table_name = resp_t[ "Tables_in_links_logs" ]
			#print( table_name )
			log_names.append( table_name )
	else:
		print( "Null response from db_logs" )

	return log_names



def inspect_log( n, log_name ):
	accept = False
	
	query = "SELECT COUNT(*) AS count FROM links_logs.`%s`;" % log_name
	#print( query )
	resp = db_logs.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		#print( count )
		if count == 0:
			print( "%3d: %s count = %d, skip" % ( n, log_name, count ) )
			return
	
	"""
	query = "SELECT id_source, COUNT(*) AS count FROM links_logs.`%s` GROUP BY id_source;" % log_name
	#print( query )
	resp = db_logs.query( query )
	if resp is not None:
		ndict = len( resp )
		#print( ndict, resp )
		print( "%3d: %s inspect" % ( n, log_name ) )
		for t in range( ndict ):
			count = resp[ t ][ "count" ]
			id_source = resp[ t ][ "id_source" ]
			print( "     number of log records for id_source %3d is %s" % ( id_source, count ) )

		msg = "%3d: %s include in report? [Y,n]" % ( n, log_name )
		answer = input( msg )
		if answer == '' or answer == 'Y' or answer == 'y':
			accept = True
	"""
	
	# only skip 0 counts, accept the rest
	print( "%3d: %s count = %d, accept" % ( n, log_name, count ) )
	accept = True
	
	return accept



def select_log_names( db_logs, begin_date, end_date ):
	if debug: print( "select_log_names()" )
	all_names = get_log_names( db_logs )
	log_names = []	# accepted log names
	nnames = len( all_names )
	
	for n in range( nnames ):
		log_name = all_names[ n ]
		if not log_name.startswith( "log-" ):
			print( "%3d: %s not a log table we want, skip" % ( n, log_name ) )
			continue	# skip this one
	
		log_date_str = log_name[ 4:14 ]
		#print( log_date_str )
	
		log_date = validate_date( log_date_str )
		if log_date is None:
			continue	# skip this one
		
		if log_date < begin_date:
			print( "%3d: %s too old, skip" % ( n, log_name ) )
		elif log_date > end_date:
			print( "%3d: %s too new, skip" % ( n, log_name ) )
		else:
			#print( "%3d: %s inspect" % ( n, log_name ) )
			accept = inspect_log( n, log_name )
			if accept:
				log_names.append( log_name )
			
	return log_names



def none2empty( var ):
	if var is None or var == "None" or var == "null":
		var = ""
	return var



def process_logs( log_names ):
	if debug: print( "process_logs()" )
	print( "\nprocessing selected logs" )
	nnames = len( log_names )
	fields = "id_log, id_source, archive, location, reg_type, date, sequence, role, guid, "
	fields += "reg_key, pers_key, report_class, report_type, content, date_time"
	
	clause = ""
	for xtype in x_codes:		# where column 'content' contains 'standard_code= 'x'
		if clause == "":
			clause = "report_type = %d" % xtype
		else:
			clause += " OR report_type = %d" % xtype
	#print( clause )
	
	time0 = time()		# seconds since the epoch
	for n in range( nnames ):
		log_name = log_names[ n ]
		print( log_name )
		
		time1 = time()		# seconds since the epoch
		query = "SELECT %s FROM links_logs.`%s` WHERE NOT (%s);" % ( fields, log_name, clause )
		print( query )
		
		resp = db_logs.query( query )
		
		str_elapsed = format_secs( time() - time1 )
		print( "query took %s" % str_elapsed )
		
		if resp is not None:
			nrec = len( resp )
			print( "number of records in table %s: %d" % ( log_name, nrec ) )
			
			for r in range( nrec ):
				if ( r > 0 and ( r + chunk ) % chunk == 0 ):
					print( "%d-of-%d records processed" % ( r, nrec ) )
				
				rec = resp[ r ]
				if debug: print( "record %d-of-%d" % ( r+1, nrec ) )
				if debug: print( rec )
				
				id_log       = none2empty( rec[ "id_log" ] )
				id_source    = none2empty( rec[ "id_source" ] )
				archive      = none2empty( rec[ "archive" ] )
				location     = none2empty( rec[ "location" ] )
				reg_type     = none2empty( rec[ "reg_type" ] )
				date         = none2empty( rec[ "date" ] )
				sequence     = none2empty( rec[ "sequence" ] )
				role         = none2empty( rec[ "role" ] )
				guid         = none2empty( rec[ "guid" ] )
				reg_key      = none2empty( rec[ "reg_key" ] )
				pers_key     = none2empty( rec[ "pers_key" ] )
				report_class = none2empty( rec[ "report_class" ] )
				report_type  = none2empty( rec[ "report_type" ] )
				content      = none2empty( rec[ "content" ] )
				date_time    = str( none2empty( rec[ "date_time" ] ) )
				
				if pers_key == '': 
					pers_key = 0	# declared as INTEGER
				
				if debug:
					print( "id_log       = %s" % id_log )
					print( "id_source    = %s" % id_source  )
					print( "archive      = %s" % archive )
					print( "location     = %s" % location )
					print( "reg_type     = %s" % reg_type )
					print( "date         = %s" % date )
					print( "sequence     = %s" % sequence )
					print( "role         = %s" % role )
					print( "guid         = %s" % guid )
					print( "reg_key      = %s" % reg_key )
					print( "pers_key     = %s" % pers_key )
					print( "report_class = %s" % report_class )
					print( "report_type  = %s" % report_type )
					print( "content      = %s" % content )
					print( "date_time    = %s" % date_time )
				
				# without id_log (also pk of ERROR_STORE table)
				# add flag = 1
				er_fields  = fields[ 8: ]
				er_fields += ", flag"
				
				flag = 1
				er_values = ( id_source, archive, location, reg_type, date, sequence, role, guid, 
					reg_key, pers_key, report_class, report_type, content, date_time, flag )
				
				# Using 'IGNORE' to ignore violations of the unique_index constraint
				er_query = "INSERT IGNORE INTO links_logs.ERROR_STORE ( %s ) VALUES %s;" % ( er_fields, er_values )
				if debug: print ( er_query )
				er_resp = db_logs.insert( er_query )
				if er_resp is not None:
					print( "er_resp:", er_resp )
			
			print( "%d-of-%d records processed" % ( nrec, nrec ) )
	
	table = "ERROR_STORE"
	query = "SELECT COUNT(*) AS count FROM links_logs.`%s`;" % table
	#print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "number of records in table %s: %d" % ( table, count ) )

	str_elapsed = format_secs( time() - time0 )
	print( "importing took %s" % str_elapsed )



def validate_date( date_string ):
	date = None
	
	try: 
		date = parse( date_string )
	except ValueError:
		print( "validate_date() ValueError: %s" % date_string )

	return date



def get_date_limit( prompt, date_default ):
	print( prompt )
	date_str = input( "yyyy-mm-dd (%s) " % date_default )
	if date_str == "":
		date_str = date_default
	print( "date: %s" % date_str )
	
	date = validate_date( date_str )
	if date is None:
		print( "%s date is not a valid date" % date )
	
	return date



def get_date_limits():
	prompt = "Begin date for inclusion of log tables: "
	begin_date = get_date_limit( prompt, begin_date_default )
	
	if begin_date is not None:
		prompt = "End date for inclusion of log tables: "
		end_date = get_date_limit( prompt, end_date_default )
	
	if begin_date > end_date:
		print( "Begin date exceeds end date" )
		begin_date = end_date = None
		
	return begin_date, end_date



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
# format_secs()



if __name__ == "__main__":
	print( "import_logs.py" )
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	
	config_path = os.path.join( os.getcwd(), "import_logs.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )

	begin_date, end_date = get_date_limits()
	if begin_date is None or end_date is None:
		print( "EXIT" )
		sys.exit( 1 )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	HOST_REF   = config.get( "HOST_REF" )
	USER_REF   = config.get( "USER_REF" )
	PASSWD_REF = config.get( "PASSWD_REF" )
	
	db = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )

	db_ref = Database( host = HOST_REF, user = USER_REF, passwd = PASSWD_REF, dbname = DBNAME_REF )
	
	db_logs = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	
#	db_check( db )

	log_names = select_log_names( db_logs, begin_date, end_date )
	process_logs( log_names )
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	str_elapsed = format_secs( time() - time0 )
	print( "Importing Logs took %s" % str_elapsed )

# [eof]
