#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		links_counts.py
Version:	0.2
Goal:		Count and compare links original & cleaned record counts

29-Mar-2016 Created
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
import socket
import yaml

from time import time

debug = False


def registration_counts( debug, host, db_ref, db_links ):
	if debug: print( "registration_counts" )
	
	sources = {}
	
	# get the number of registration records per source from links_original
	table = "registration_o"
	query_id = "query_o"
	query_o = "SELECT id_source, COUNT(*) AS count FROM links_original." + table + " GROUP BY id_source" + ";"
#	query_o = "SELECT id_source, registration_maintype, COUNT(*) AS count FROM links_original." + table + " GROUP BY id_source, registration_maintype" + ";"
	print( query_o )
	
	resp_o = db_links.query( query_o )
	if debug: print( resp_o )
	nsources_o = len( resp_o )
	if debug: print( "registration_o: %d sources" % nsources_o )

	for s in range( nsources_o ):
		source = {}
		d = resp_o[ s ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		if debug: print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
		
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		sources[ id_source ] = [ entry ]	# clean counts dict will be added to array
	
	if debug: print( sources )
	
	
	# get the number of registration records per source from links_cleaned
	table = "registration_c"
	query_id = "query_c"
	query_c = "SELECT id_source, COUNT(*) AS count FROM links_cleaned." + table + " GROUP BY id_source" + ";"
#	query_c = "SELECT id_source, registration_maintype, COUNT(*) AS count FROM links_cleaned." + table + " GROUP BY id_source, registration_maintype" + ";"
	print( query_c )
	
	resp_c = db_links.query( query_c )
	if debug: print( resp_c )
	nsources_c = len( resp_c )
	if debug: print( "registration_c: %d sources\n" % nsources_c )
	
	for s_c in range( nsources_c ):
		source = {}
		d = resp_c[ s_c ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		if debug: print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
		
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources.get( id_source )
		if array is None:
			sources[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources )
	
	
	now = datetime.datetime.now()
	print( "" )
	if host == "localhost":
		hostname = socket.getfqdn()
		print( "host: %s (%s), date: %s" % ( host, hostname, str( now.strftime( "%Y-%m-%d" ) ) ) )
	else:
		print( "host: %s, date: %s" % ( host, str( now.strftime( "%Y-%m-%d" ) ) ) )
	print( "============================================================================" )
	print( "     id        short        registration_o  registration_c  --- o/c loss ---" )
	print( " # source  archive name      record count    record count   diff     procent" )
	print( "----------------------------------------------------------------------------" )
	
	n = 0
	for key in sorted( sources ):
		n += 1
		tables = sources[ key ]
		#print( key, tables )
		
		id_source = key
		id_source_str = str( id_source )
		
		"""
		try:
			name = archive_names[ id_source_str ]
		except:
			name = ""
		"""
		source_name, short_name = get_archive_name( db_ref, id_source_str )
		name = short_name
		
		count_o = 0
		count_c = 0
	
		for t in tables:
			t_name = t.get( "table" )
			if t_name == "registration_o":
				count_o = t.get( "count" )
			elif t_name == "registration_c":
				count_c = t.get( "count" )
		
		orig  = count_o
		clean = count_c
				
		diff = orig - clean
		if orig != 0:
			procent = 100.0 * float( diff ) / ( orig )
		else:
			procent = None
		
		if procent is not None:
			print( "%2d %4s   %-20s %7d         %7d   %7d    %6.2f %%" % 
				( n, id_source_str, name, orig, clean, diff, procent ) )
		else:
			procent_str = "     "
			print( "%2d %4s   %-20s %7d         %7d   %7d    %5s" % 
				( n, id_source_str, name, orig, clean, diff, procent_str ) )
	
	print( "============================================================================\n" )
# registration_counts()



def person_counts( debug, host, db_ref, db_links ):
	if debug: print( "person_counts" )
	
	sources = {}
	
	# get the number of person records per source from links_original
	table = "person_o"
	query_id = "query_o"
	query_o = "SELECT id_source, COUNT(*) AS count FROM links_original." + table + " GROUP BY id_source" + ";"
	print( query_o )
	
	resp_o = db_links.query( query_o )
	if debug: print( resp_o )
	nsources_o = len( resp_o )
	if debug: print( "person_o: %d sources" % nsources_o )

	for s in range( nsources_o ):
		source = {}
		d = resp_o[ s ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		if debug: print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
		
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		sources[ id_source ] = [ entry ]	# clean counts dict will be added to array
	
	if debug: print( sources )
	
	
	# get the number of person records per source from links_cleaned
	table = "person_c"
	query_id = "query_c"
	query_c = "SELECT id_source, COUNT(*) AS count FROM links_cleaned." + table + " GROUP BY id_source" + ";"
	print( query_c )
	
	resp_c = db_links.query( query_c )
	if debug: print( resp_c )
	nsources_c = len( resp_c )
	if debug: print( "person_c: %d sources\n" % nsources_c )
	
	for s_c in range( nsources_c ):
		source = {}
		d = resp_c[ s_c ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "clean" ] = count
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
		
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources.get( id_source )
		if array is None:
			sources[ id_source ] = [ entry ]
		else:
			array.append( entry )
		
	if debug: print( sources )
	
	now = datetime.datetime.now()
	
	print( "" )
	if host == "localhost":
		hostname = socket.getfqdn()
		print( "host: %s (%s), date: %s" % ( host, hostname, str( now.strftime( "%Y-%m-%d" ) ) ) )
	else:
		print( "host: %s, date: %s" % ( host, str( now.strftime( "%Y-%m-%d" ) ) ) )
	print( "================================================================================" )
	print( "     id        short           person_o          person_c     --- o/c loss ---" )
	print( " # source  archive name      record count      record count   diff     procent" )
	print( "--------------------------------------------------------------------------------" )
	
	n = 0
	for key in sorted( sources ):
		n += 1
		tables = sources[ key ]
		#print( key, tables )
		
		id_source = key
		id_source_str = str( id_source )
		
		"""
		try:
			name = archive_names[ id_source_str ]
		except:
			name = ""
		"""
		source_name, short_name = get_archive_name( db_ref, id_source_str )
		name = short_name
		
		count_o = 0
		count_c = 0
		count_b = 0
		
		for t in tables:
			t_name = t.get( "table" )
			if t_name == "person_o":
				count_o = t.get( "count" )
			elif t_name == "person_c":
				count_c = t.get( "count" )
			elif t_name == "links_base":
				count_b = t.get( "count" )
		
		orig  = count_o
		clean = count_c
		base  = count_b
		
		diff_oc = orig - clean
		if orig != 0:
			procent_oc = 100.0 * float( diff_oc ) / ( orig )
			procent_oc_str = "%6.2f %%" % procent_oc
		else:
			procent_oc_str = "     "
		
		diff_cb = clean - base
		if clean != 0:
			procent_cb = 100.0 * float( diff_cb ) / ( clean )
			procent_cb_str = "%6.2f %%" % procent_cb
		else:
			procent_cb_str = "     "
		
		print( "%2d %4s   %-20s  %8d          %8d   %7d    %5s" % 
			( n, id_source_str, name, orig, clean, diff_oc, procent_oc_str ) )
			
	print( "================================================================================\n" )

	return sources
# person_counts()



def base_counts( debug, host, db_ref, db_links, sources_person ):
	if debug: print( "base_counts" )
	
	sources_base = {}
	
	# Get the number of records per source from links_base.  
	# different counts between person_c and links_base are caused role and/or registration_maintype problems. 
	# A too large difference indicates a potential problem with cleaning and/or prematching. 
	table = "links_base"
	query_id = "query_b1"
	query_b1  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_b1 += " GROUP BY id_source" + ";"
	print( query_b1 )
	
	resp_b1 = db_links.query( query_b1 )
	if debug: print( resp_b1 )
	nsources_b1 = len( resp_b1 )
	if debug: print( "links_base: %d sources\n" % nsources_b1 )
	
	for s_b1 in range( nsources_b1 ):
		source = {}
		d = resp_b1[ s_b1 ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )

	
	# Get the number of records per source from links_base, 
	# where the ego_familyname is missing; these are not used for matching. 
	table = "links_base"
	query_id = "query_be"
	query_be  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_be += " WHERE ego_familyname IS NULL OR ego_familyname = 0"
	query_be += " GROUP BY id_source" + ";"
	print( query_be )
	
	resp_be = db_links.query( query_be )
	if debug: print( resp_be )
	nsources_be = len( resp_be )
	if debug: print( "links_base: %d sources\n" % nsources_be )
	
	for s_be in range( nsources_be ):
		source = {}
		d = resp_be[ s_be ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )
	
	
	# Get the number of records per source from links_base, 
	# where the registration_days is missing; these are not used for matching. 
	table = "links_base"
	query_id = "query_bd"
	query_bd  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_bd += " WHERE registration_days IS NULL OR registration_days = 0"
	query_bd += " GROUP BY id_source" + ";"
	print( query_bd )
	
	resp_bd = db_links.query( query_bd )
	if debug: print( resp_bd )
	nsources_bd = len( resp_bd )
	if debug: print( "links_base: %d sources\n" % nsources_bd )

	for s_bd in range( nsources_bd ):
		source = {}
		d = resp_bd[ s_bd ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )


	# Get the number of records per source from links_base. 
	# The requirements for ego_familyname and registration_days may lead to 
	# different counts between person_c and links_base. 
	# A too large difference indicates a potential problem with cleaning and/or prematching. 
	table = "links_base"
	query_id = "query_b2"
	query_b2  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_b2 += " WHERE ego_familyname <> 0 AND registration_days IS NOT NULL AND registration_days <> 0"
	query_b2 += " GROUP BY id_source" + ";"
	print( query_b2 )

	resp_b2 = db_links.query( query_b2 )
	if debug: print( resp_b2 )
	nsources_b2 = len( resp_b2 )
	if debug: print( "links_base: %d sources\n" % nsources_b2 )

	for s_b2 in range( nsources_b2 ):
		source = {}
		d = resp_b2[ s_b2 ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
	#	#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )


	now = datetime.datetime.now()
	print( "" )
	if host == "localhost":
		hostname = socket.getfqdn()
		print( "host: %s (%s), date: %s" % ( host, hostname, str( now.strftime( "%Y-%m-%d" ) ) ) )
	else:
		print( "host: %s, date: %s" % ( host, str( now.strftime( "%Y-%m-%d" ) ) ) )
	print( "===========================================================================================" )
	print( "     id        short         links_base   -- c/b loss --  missing  missing  -- b/b loss --" )
	print( " # source  archive name     record count  diff   procent  ego fam  days sb  diff   procent" )
	print( "-------------------------------------------------------------------------------------------" )
		
	n = 0
	for key in sorted( sources_base ):
		n += 1
		
		tables_person = sources_person[ key ]
		count_c = 0
		for t in tables_person:
			t_name = t.get( "table" )
			if t_name == "person_c":
				count_c = t.get( "count" )
		clean = count_c
		
		queries_base = sources_base[ key ]
		#print( key, queries_base )
		
		id_source = key
		id_source_str = str( id_source )
		
		"""
		try:
			name = archive_names[ id_source_str ]
		except:
			name = ""
		"""
		source_name, short_name = get_archive_name( db_ref, id_source_str )
		name = short_name
		
		count_b1 = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_b1":
				count_b1 = q.get( "count" )
		base1 = count_b1
		
		diff_cb = clean - base1
		if clean != 0:
			procent_cb = 100.0 * float( diff_cb ) / ( clean )
			procent_cb_str = "%6.2f %%" % procent_cb
		else:
			procent_cb_str = "     "
		
		count_be = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_be":
				count_be = q.get( "count" )
		ego = count_be
		
		count_bd = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_bd":
				count_bd = q.get( "count" )
		days = count_bd
		
		count_b2 = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_b2":
				count_b2 = q.get( "count" )
		base2 = count_b2
		
		# wrong: diff_bb = ego + days, because ego and days may contain overlapping records
		diff_bb = base1 - base2
		if base1 != 0:
			procent_bb = 100.0 * float( diff_bb ) / ( base1 )
			procent_bb_str = "%6.2f %%" % procent_bb
		else:
			procent_bb_str = "     "
		
		#print( "%2d %4s   %-20s %7d %7d  %5s %7d %7d %7d  %5s" % 
		print( "%2d %4s   %-20s %8d %7d  %5s %7d %7d %7d  %5s" % 
			( n, id_source_str, name, base1, diff_cb, procent_cb_str, ego, days, diff_bb, procent_bb_str ) )

	print( "===========================================================================================\n" )
# base_counts()



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
	if debug: print( "links_counts.py" )
	
	time0 = time()		# seconds since the epoch
	
	yaml_filename = "./links_counts.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )
	
	MINIMUM_RECORD_COUNT = config_local.get( "MINIMUM_RECORD_COUNT" )
	REPORT_TYPE          = config_local.get( "REPORT_TYPE", "0" )
	
	HOST_REF   = config_main.get( "HOST_REF" )
	USER_REF   = config_main.get( "USER_REF" )
	PASSWD_REF = config_main.get( "PASSWD_REF" )
	DBNAME_REF = config_main.get( "DBNAME_REF" )
	
	print( "HOST_REF: %s" % HOST_REF )
	print( "USER_REF: %s" % USER_REF )
	print( "PASSWD_REF: %s" % PASSWD_REF )
	print( "DBNAME_REF: %s" % DBNAME_REF )
	
	HOST_LINKS   = config_main.get( "HOST_LINKS" )
	USER_LINKS   = config_main.get( "USER_LINKS" )
	PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
	DBNAME_LINKS = "links_original"
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	print( "USER_LINKS: %s" % USER_LINKS )
	print( "PASSWD_LINKS: %s" % PASSWD_LINKS )
	print( "DBNAME_LINKS: %s" % DBNAME_LINKS )
	
	main_dir = os.path.dirname( YAML_MAIN )
	sys.path.insert( 0, main_dir )
	from hsn_links_db import Database, format_secs, get_archive_name
	
	print( "Connecting to database at %s" % HOST_REF )
	db_ref = Database( host = HOST_REF,   user = USER_REF,   passwd = PASSWD_REF,   dbname = DBNAME_REF )
	
	print( "Connecting to database at %s" % HOST_LINKS )
	db_links = Database( host = HOST_LINKS , user = USER_LINKS , passwd = PASSWD_LINKS , dbname = DBNAME_LINKS )

	registration_counts( debug, HOST_LINKS, db_ref, db_links )
	sources_person = person_counts( debug, HOST_LINKS, db_ref, db_links )
	base_counts( debug, HOST_LINKS, db_ref, db_links, sources_person )

# [eof]
