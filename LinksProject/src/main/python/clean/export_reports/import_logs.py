#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		import_logs.py
Version:	0.2
Goal:		Collect error log tables into a single table ERROR_STORE. 
			The records are written with flag value = 1. 
Database:	Create table links_logs.ERROR_STORE from schema: 
			$ mysql --user=USER_LINKS --password=PASSWD_LINKS links_logs < ERROR_STORE.schema.sql

05-Sep-2016 Created
11-Jun-2021 Chunk processing log tables, can be big
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

from dateutil.parser import parse		# $ pip install python-dateutil
from time import time

debug = False
chunk_default = 100000		# show progress in processing records

begin_date_default = "2021-01-01"
end_date_default   = "2021-12-31"

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

# excluded 'type' values of ref_report with 'standard_code= 'x''.
# now from yaml config
#x_codes_default = [ 21, 31, 41, 51, 61, 71, 81, 91, 141, 251, 1009, 1109 ]


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
# get_log_names()



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
# inspect_log()



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
# select_log_names()



def none2empty( var ):
	if var is None or var == "None" or var == "null":
		var = ""
	return var
# none2empty()



def process_logs( log_names, chunk ):
	if debug: print( "process_logs()" )
	print( "\nprocessing selected logs" )
	
	nnames = len( log_names )
	fields = "id_log, id_source, archive, scan_url, location, reg_type, date, sequence, role, guid, "
	fields += "reg_key, pers_key, report_class, report_type, content, date_time"
	
	nrec_tot = 0
	inserted_tot = 0
	
	for n in range( nnames ):
		time0 = time()		# seconds since the epoch
		
		log_name = log_names[ n ]
		print( "table %d-of-%d %s" % ( n+1, nnames, log_name ) )
		
		if len( x_codes ) > 0:
			x_tuple = tuple( x_codes )
			query = "SELECT COUNT(*) as count FROM links_logs.`%s` WHERE NOT report_type IN %s;" % ( log_name, x_tuple )
		else:		# no exclusions
			query = "SELECT COUNT(*) as count FROM links_logs.`%s`;" % ( log_name )
		#print( query )
		
		count_file = 0
		resp = db_logs.query( query )
		if resp is not None:
			count_file = resp[ 0 ].get( "count", 0 )
		if debug: print( "number of records in log table = %d" % count_file )
		
		nchunks = 0
		if count_file > 0:
			nremain = count_file % chunk
			nchunks = int( ( count_file + chunk ) / chunk )
		print( "nchunks = %d" % nchunks )
		
		nrec_file = 0
		inserted_file = 0
		
		for n in range( nchunks ):
			print( "nchunk %d-of-%d" % ( n+1, nchunks ) )
			offset = n * chunk
		
			time1 = time()		# seconds since the epoch
			
			if len( x_codes ) > 0:
				x_tuple = tuple( x_codes )
				query = "SELECT %s FROM links_logs.`%s` WHERE NOT report_type IN %s" % ( fields, log_name, x_tuple )
			else:		# no exclusions
				query = "SELECT %s FROM links_logs.`%s`" % ( fields, log_name )
			query += " LIMIT %d OFFSET %d;" % ( chunk, offset )
			print( query )
			
			resp = db_logs.query( query )
			
			str_elapsed = format_secs( time() - time1 )
			print( "query took %s" % str_elapsed )
			
			
			if resp is not None:
				nrec = len( resp )
				nrec_file += nrec
				print( "number of records in chunk %s: %d" % ( log_name, nrec ) )
				
				for r in range( nrec ):
					if ( r > 0 and ( r + chunk ) % chunk == 0 ):
						print( "%d-of-%d records processed" % ( r, nrec ) )
					
					rec = resp[ r ]
					if debug: print( "record %d-of-%d" % ( r+1, nrec ) )
					if debug: print( rec )
					
					id_log       = none2empty( rec[ "id_log" ] )
					id_source    = none2empty( rec[ "id_source" ] )
					archive      = none2empty( rec[ "archive" ] )
					scan_url     = none2empty( rec[ "scan_url" ] )
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
						print( "scan_url     = %s" % scan_url )
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
					er_values = ( id_source, archive, scan_url, location, reg_type, date, sequence, role, guid, 
						reg_key, pers_key, report_class, report_type, content, date_time, flag )
					
					# Using 'IGNORE' to ignore violations of the unique_index constraint
					es_query = "INSERT IGNORE INTO links_logs.ERROR_STORE ( %s ) VALUES %s;" % ( er_fields, er_values )
					if debug: print ( es_query )
					es_resp = db_logs.insert( es_query )
					if debug: print( "es_resp: %s" % es_resp )
					if es_resp is not None:
						inserted_file += es_resp
				
			print( "%d-of-%d records processed from chunk" % ( nrec, nrec ) )
			print( "%d records inserted in ERROR_STORE" % inserted_file )
			
		inserted_tot += inserted_file
		if count_file != nrec_file:
			print( "bookkeeping error: count_file: %d, nrec_file: %d" % ( count_file, nrec_file ) )
		print( "from %d records %d were actually inserted" % ( nrec_file, inserted_file ) )
		
		str_elapsed = format_secs( time() - time0 )
		print( "importing table took %s\n" % str_elapsed )
	
	print( "total records inserted: %d" % inserted_tot )
	
	table = "ERROR_STORE"
	query = "SELECT COUNT(*) AS count FROM links_logs.`%s`;" % table
	#print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "number of records in table %s: %d" % ( table, count ) )
# process_logs()



def validate_date( date_string ):
	date = None
	
	try: 
		date = parse( date_string )
	except ValueError:
		print( "validate_date() ValueError: %s" % date_string )

	return date
# validate_date()



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
# get_date_limit()



def get_date_limits( begin_date, end_date ):
	prompt = "Begin date for inclusion of log tables: "
	begin_date = get_date_limit( prompt, begin_date )
	
	if begin_date is not None:
		prompt = "End date for inclusion of log tables: "
		end_date = get_date_limit( prompt, end_date )
	
	if begin_date > end_date:
		print( "Begin date exceeds end date" )
		begin_date = end_date = None
		
	return begin_date, end_date
# get_date_limits()



def get_yaml_config( yaml_filename ):
	config = {}
	print( "Trying to load the yaml config file: %s" % yaml_filename )
	
	if yaml_filename.startswith( "./" ):	# look in startup directory
		yaml_filename = yaml_filename[ 2: ]
		config_path = os.path.join( sys.path[ 0 ], yaml_filename )
	
	else:
		try:
			LINKS_HOME = os.environ[ "LINKS_HOME" ]
		except:
			LINKS_HOME = ""
		
		if not LINKS_HOME:
			print( "environment variable LINKS_HOME not set" )
		else:
			print( "LINKS_HOME: %s" % LINKS_HOME )
		
		config_path = os.path.join( LINKS_HOME, yaml_filename )
	
	print( "yaml config path: %s" % config_path )
	
	try:
		config_file = open( config_path )
		config = yaml.safe_load( config_file )
	except:
		etype = sys.exc_info()[ 0:1 ]
		value = sys.exc_info()[ 1:2 ]
		print( "%s, %s\n" % ( etype, value ) )
		sys.exit( 1 )
	
	return config
# get_yaml_config()



if __name__ == "__main__":
	print( "import_logs.py" )
	time0 = time()		# seconds since the epoch
	
	yaml_filename = "./import_logs.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	x_codes = config_local.get( "EXCLUDE_ERR" )
	print( "Excluded error codes for log import: %s" % x_codes )
	
	begin_date = config_local.get( "BEGIN_DATE", begin_date_default )
	end_date   = config_local.get( "END_DATE",   end_date_default )
	
	begin_date, end_date = get_date_limits( begin_date, end_date )
	if begin_date is None or end_date is None:
		print( "EXIT" )
		sys.exit( 1 )
	
	chunk = config_local.get( "CHUNK", chunk_default )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )
	
	HOST_LINKS   = config_main.get( "HOST_LINKS" )
	USER_LINKS   = config_main.get( "USER_LINKS" )
	PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	print( "USER_LINKS: %s" % USER_LINKS )
	print( "PASSWD_LINKS: %s" % PASSWD_LINKS )
	
	HOST_REF   = config_main.get( "HOST_REF" )
	USER_REF   = config_main.get( "USER_REF" )
	PASSWD_REF = config_main.get( "PASSWD_REF" )
	
	print( "HOST_REF: %s" % HOST_REF )
	print( "USER_REF: %s" % USER_REF )
	print( "PASSWD_REF: %s" % PASSWD_REF )
	
	main_dir = os.path.dirname( YAML_MAIN )
	sys.path.insert( 0, main_dir )
	from hsn_links_db import Database, format_secs, get_archive_name
	
	db = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )

	db_ref = Database( host = HOST_REF, user = USER_REF, passwd = PASSWD_REF, dbname = DBNAME_REF )
	
	db_logs = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	

	log_names = select_log_names( db_logs, begin_date, end_date )
	process_logs( log_names, chunk )
	
	str_elapsed = format_secs( time() - time0 )
	print( "Importing Logs took %s" % str_elapsed )

# [eof]
