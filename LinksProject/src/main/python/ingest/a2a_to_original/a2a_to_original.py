#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		a2a_to_original.py
Version:	0.4
Goal:		Read the sql file with MySQL queries to fill the 2 table of 
			links_original from the links_a2a tables. 
			Show progress by showing the queries one-by-one. 

04-Dec-2014	Created
18-Jan-2016	Update for Python-3
10-Apr-2017	Get sources/rmtypes from links_a2a, and delete them accordingly from links_original
19-May-2021	Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import arrow
import datetime
import MySQLdb
import os
import sys
import yaml

from time import time

debug = False
keep_comments = False

# db
HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""

sql_dirname  = os.path.dirname( os.path.realpath( __file__ ) )
sql_filename = "a2a_to_original.sql"
sql_path     = os.path.join( sql_dirname, sql_filename )


def db_check( db ):
	print( "db_check()" )

	tables = [ "a2a", "event", "object", "person", "person_o_temp", "person_profession", 
			  "registration_o_temp", "relation", "remark", "source", "source_sourceavailablescans_scan" ]

	print( "table row counts:" )
	for table in tables:
		query = """SELECT COUNT(*) FROM %s""" % table
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
# db_check()



def queries( db, log ):
	print( "queries()" )
	log.write( "queries()\n" )
	try:
		sql_file = open( sql_path, 'r' )
		if debug: print( sql_path )
	except:
		etype = sys.exc_info()[0:1]
		value = sys.exc_info()[1:2]
		log.write( "%s\n" % sql_path )
		log.write( "open sql file failed: %s, %s\n" % ( etype, value ) )
		exit( 1 )

	nline = 0
	query = ""
	queries = []
	nqueries = 0

	while True:
		nline += 1
		line = sql_file.readline()			# includes trailing newline
		if len( line ) == 0:
			break

		line = line.strip()					# remove newline
		size = len( line )

		if size == 0:
			if debug: log.write( "line %d empty\n" % nline )
		elif line.startswith( "--" ):
			if debug: log.write( "sql comment line %d: %s\n" % ( nline, line ) )
			if keep_comments:		# keep comment line for informational purposes in log file
				queries.append( line )
		else:
			# proper query; reckon with multi-line queries
			if debug: log.write( "line %d query: %s\n" % ( nline, line ) )
			query += line
			query += ' '

			if line.endswith( ';' ):
				nqueries += 1
				if debug: log.write( "end query %d\n\n" % nqueries)
				queries.append( query )
				query = ""

	log.flush()

	log.write( "\n%d queries in %s\n" % ( nqueries, sql_path ) )
	nq = 0		# 'real' queries, omitting comment lines
	for q in range( len( queries ) ):
		query = queries[ q ]

		if query.startswith( "--" ):
			log.write( "%s\n" % query )
		else:
			t1 = time()		# seconds since the epoch
			nq += 1
			print( "query %d-of-%d ..." % ( nq, nqueries ) )
			log.write( "query %d-of-%d ...\n" % ( nq, nqueries ) )
			log.write( "%s\n" % query )
			log.flush()

			resp = db.query( query )
			if resp: 
				log.write( "resp: %s\n" % str( resp ) )
				#print( resp )

			info = db.info()
			if info is not None: 
				log.write( "info: %s\n" % str( info ) )

			str_elapsed = format_secs( time() - t1 )
			log.write( "query in %s\n\n" % str_elapsed )
			log.flush()
# queries()



def sources_from_a2a( db, log ):
	print( "sources_from_a2a()" )
	log.write( "sources_from_a2a()\n" )
	# links_a2a.source.archive    => id_source
	# links_a2a.source.collection => registration_maintype
	
	db_name = "links_a2a"
	table = "source"
	query_sel  = "SELECT archive AS source, collection AS rmtype, COUNT(*) AS count FROM %s.%s " % ( db_name, table )
	query_sel += "GROUP BY source, rmtype ORDER BY source, rmtype;"
#	query_sel += "GROUP BY archive, collection ORDER BY archive, collection;"
	log.write( "%s\n" % query_sel )
	resp_sel = db.query( query_sel )
	
	rmtypes = []	# source + rmtype pairs
	for dict_sel in resp_sel:
		log.write( "%s\n" % str( dict_sel )  )
		rmtypes.append( dict_sel )
	
	return rmtypes
# sources_from_a2a()



def delete_from_orig( db, log, rmtypes ):
	print( "delete_from_orig()" )
	log.write( "delete_from_orig()\n" )
	
	db_name = "links_original"
	
	for dict_src in rmtypes:
		source = int( dict_src[ "source" ] )
		rmtype = int( dict_src[ "rmtype" ] )
		
		table = "registration_o"
		query_r  = "DELETE FROM %s.%s " % ( db_name, table )
		query_r += "WHERE id_source = %d AND registration_maintype = %d;" % ( source, rmtype )
		log.write( "%s\n" % query_r )
		resp = db.delete( query_r )
		if resp:
			log.write( "%s\n" % resp )
		info = db.info()
		if info is not None: 
			log.write( "info: %s\n" % str( info ) )
		
		table = "person_o"
		query_p  = "DELETE FROM %s.%s " % ( db_name, table )
		query_p += "WHERE id_source = %d AND registration_maintype = %d;" % ( source, rmtype )
		log.write( "%s\n" % query_p )
		resp = db.delete( query_p )
		if resp:
			log.write( "%s\n" % resp )
		info = db.info()
		if info is not None: 
			log.write( "info: %s\n" % str( info ) )
	
	for table in [ "registration_o", "person_o" ]:
		query = "SELECT COUNT(*) AS count FROM %s.%s " % ( db_name, table )
		log.write( "%s\n" % query )
		resp = db.query( query )
		if resp:
			count = resp[ 0 ][ "count" ]
			log.write( "%d records in %s\n" % ( count, table ) )
			
			if count == 0:
				log.write( "Empty table, resetting AUTO_INCREMENT\n" )
				query = "ALTER TABLE %s.%s AUTO_INCREMENT = 1" % ( db_name, table )
				log.write( "%s\n" % query )
				resp = db.execute( query )
				if resp:
					log.write( "%s\n" % str( resp ) )
	
# delete_from_orig()



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
	if debug: print( "a2a_to_original.py" )
	
	time0 = time()		# seconds since the epoch
	
	yaml_filename = "./a2a_to_original.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )

	cur_dirname = os.path.dirname( os.path.realpath( __file__ ) )
	log_dirname = os.path.join( cur_dirname, "log" )

	# ensure the log directory exists
	if not os.path.exists( log_dirname  ):
		os.makedirs( log_dirname  )

	timestamp    = arrow.now().format( "YYYY.MM.DD-HH:mm:ss" )
	log_filename = "A2O-%s.log" % timestamp
	log_path     = os.path.join( log_dirname, log_filename )

	try:
		log = open( log_path, 'w' )
		print( "logging to: %s" % log_path )
	except:
		etype = sys.exc_info()[0:1]
		value = sys.exc_info()[1:2]
		print( log_path )
		print( "open log file failed: %s" % value )
		exit( 1 )

	t1 = time()
	
	HOST_LINKS   = config_main.get( "HOST_LINKS" )
	USER_LINKS   = config_main.get( "USER_LINKS" )
	PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
	DBNAME_LINKS = "links_a2a"
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	print( "USER_LINKS: %s" % USER_LINKS )
	print( "PASSWD_LINKS: %s" % PASSWD_LINKS )
	print( "DBNAME_LINKS: %s" % DBNAME_LINKS )
	
	main_dir = os.path.dirname( YAML_MAIN )
	sys.path.insert( 0, main_dir )
	from hsn_links_db import Database, format_secs, get_archive_name
	
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )

	db_check( db_links )

	rmtypes = sources_from_a2a( db_links, log )
	delete_from_orig( db_links, log, rmtypes )

	queries( db_links, log )		# queries from existing sql file
	print( "see for details: %s" % log_path )
	
	str_elapsed = format_secs( time() - t1 )
	log.write( "Done in %s\n" % str_elapsed )

# [eof]
