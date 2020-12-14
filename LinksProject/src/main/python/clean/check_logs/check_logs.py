#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		check_logs.py
Version:	0.2
Goal:		Find report_type records, and optionally delete log tables

25-Nov-2020 Created
14-Dec-2020 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import os
import sys
import datetime
import MySQLdb
import re
import yaml

from time import time

debug = False


def get_table_list( db ):
	if debug: print( "get_table_list(" )
	log_tables = []
	query = "SHOW TABLES FROM links_logs LIKE 'log-%'"
	if debug: print( query )
	resp = db.query( query )
	for rec in resp:
		#if debug: print( rec )
		log_table = rec[ "Tables_in_links_logs (log-%)" ]
		log_tables.append( log_table )
	
	print( "Number of log_tables: %d" % len( log_tables ) )
	return log_tables
# get_table_list()



def check_table_list( db, table_list, min_rec_count ):
	if debug: print( "check_table_list()" )
	
	ntables = len( table_list  )
	keep_list = []
	ndeleted = 0
	
	for t, table_name in enumerate( table_list ):
		#if debug: print( table_name )
		query = "SELECT COUNT(*) AS count FROM links_logs.`%s`" % table_name
		resp = db.query( query )
		count = resp[ 0 ][ "count" ]
		print( "%d-of-%d table: %s, # of records: %d" % ( t+1, ntables, table_name, count ) )
		
		if count <= min_rec_count:	# interactvely delete
			yn = input( "Delete table %s? [N,y] " % table_name )
			if yn.lower() == 'y':
				query = "DROP TABLE links_logs.`%s`" % table_name
				if debug: print( query )
				resp = db.query( query )
				if debug: print( resp )
				ndeleted += 1
			else:
				keep_list.append( table_name )
		else:
			keep_list.append( table_name )
	
	
	print( "%d tables were deleted" %  ndeleted )
	print( "%d tables remain" % len( keep_list ) )
	return keep_list
# check_table_list()



def find_report_type( db, keep_list, report_type ): 
	if debug: print( "find_report_type(()" )
	ntables = len( keep_list  )
	nskipped = 0
	nnonzero = 0
	
	for t, table_name in enumerate( keep_list ):
		if debug: print( table_name )
		query = "SELECT COUNT(*) AS count FROM links_logs.`%s` WHERE report_type = %d" % ( table_name, report_type )
		resp = db.query( query )
		for rec in resp:
			#if debug: print( rec )
			count = rec[ "count" ]
			if count == 0:
				nskipped += 1
			else:
				nnonzero += 1
				print( "%d-(%d)-of-%d table: %s, report_type = %d count: %d" % ( t+1, nnonzero, ntables, table_name, report_type, count ) )
	
	print( "%d tables skipped with 0 count of report_type = %d " % ( nskipped, report_type ) )
# find_report_type()



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
	if debug: print( "check_logs.py" )
	
	time0 = time()		# seconds since the epoch
	
	yaml_filename = "./check_logs.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )
	
	MINIMUM_RECORD_COUNT = config_local.get( "MINIMUM_RECORD_COUNT" )
	REPORT_TYPE          = config_local.get( "REPORT_TYPE", "0" )
	
	HOST_LINKS   = config_main.get( "HOST_LINKS" )
	USER_LINKS   = config_main.get( "USER_LINKS" )
	PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
	DBNAME_LINKS = "links_logs"
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	print( "USER_LINKS: %s" % USER_LINKS )
	print( "PASSWD_LINKS: %s" % PASSWD_LINKS )
	print( "DBNAME_LINKS: %s" % DBNAME_LINKS )
	
	main_dir = os.path.dirname( YAML_MAIN )
	sys.path.insert( 0, main_dir )
	from hsn_links_db import Database, format_secs
	
	print( "Connecting to database at %s" % HOST_LINKS )
	db = Database( host = HOST_LINKS , user = USER_LINKS , passwd = PASSWD_LINKS , dbname = DBNAME_LINKS )
	
	print( "MINIMUM_RECORD_COUNT: %s" % MINIMUM_RECORD_COUNT )
	table_list = get_table_list( db )
	keep_list = check_table_list( db, table_list, MINIMUM_RECORD_COUNT )
	
	print( "\nREPORT_TYPE: %s" % REPORT_TYPE )
	if REPORT_TYPE:
		find_report_type( db, keep_list, REPORT_TYPE )	
	
	str_elapsed = format_secs( time() - time0 )
	print( "\nprocessing took %s" % str_elapsed )
		
# [eof]
