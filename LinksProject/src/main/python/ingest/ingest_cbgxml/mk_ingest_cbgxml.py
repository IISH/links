#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		mk_ingest_cbgxml.py
Version:	0.3
Goal:		Ingest CBG XMl files into links_a2a db
			This script creates an ingest-%s-yyyy.mm.dd.sh shell script to be run

15-Dec-2020 Created
21-Apr-2021 Also create csv overview
04-May-2021 Yaml structure changed
09-May-2021 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import arrow
import csv
import io
import os
import sys
import datetime
import MySQLdb
import socket
import tablib
import yaml

from time import time

debug = True


def get_id_source_from_archive( db_ref, archive_name ):
	print( "get_id_source_from_archive()" )
	id_source  = None
	short_name = None
	
	query = "SELECT id_source, short_name FROM ref_source WHERE source_name = '%s'" % archive_name
	if debug: print( query )
	resp = db_ref.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if nrec == 0:
			print( "No valid record found in ref_source for archive_name = '%s'" % archive_name )
		elif nrec == 1:
			rec = resp[ 0 ]
			id_source  = rec[ "id_source" ]
			short_name = rec[ "short_name" ]
		else:
			print( "Too many archive_name records found, ignoring them all\n" )
	
	if debug and id_source:
		print( "id_source = %d, short_name = %s" % ( id_source, short_name ) )
	
	return id_source, short_name
# get_id_source_from_archive()



def split_xml_fname( xml_fname ):
	#print( "split_xml_fname()" )
	
	ghoe_type = None
	rmtype = None
	archive_name = ""
	
	root, ext = os.path.splitext( xml_fname )
	if ext != ".xml":
		print( "%s: xml_fname extension must be '.xml', but it is '%s'" % ( xml_fname, ext ) )
		return ghoe_type, rmtype, archive_name
	
	parts = root.split( '_' )
	
	if len( parts ) not in [ 4, 5 ]:	# date part: 4: yyyy-mm or 5: yyyy_mm
		print( "root: %s " % root )
		for p, part in enumerate ( parts ):
			print( "%d %s" % ( p, part ) )
		print( "%s: xml_fname must split into 4 parts, but it has %d" % ( xml_fname, len( parts ) ) )
		return ghoe_type, rmtype, archive_name
	
	prefix = archive_name = parts[ 0 ]
	if prefix != "A2A":
		print( "%s: prefix must be 'A2A', but it is '%s'" % ( xml_fname, prefix ) )
		return ghoe_type, rmtype, archive_name
	
	ghoe_type = parts[ 1 ]
	if ghoe_type == "BSG":
		rmtype = 1
	elif ghoe_type == "BSH":
		rmtype = 2
	elif ghoe_type == "BSO":
		rmtype = 3
	elif ghoe_type == "BSE":
		rmtype = 4
	else:
		print( "%s: ghoe_type must be one of 'BSG', 'BSH', 'BSE', 'BSO', but it is '%s'" % ( xml_fname, ghoe_type ) )
		return ghoe_type, rmtype, archive_name
	
	archive_name = parts[ -1 ]
	if debug: print( "archive_name: %s " % archive_name )

	return ghoe_type, rmtype, archive_name
# split_xml_fname()



def process_xml( db_ref, host_links, user_links, passwd_links, a2aperl_dir, cbgxml_dir, cbgxml_list, cbgxml_skip ):
	print( "process_xml()" )
	print( "cbgxml_dir:  %s" % cbgxml_dir )
	print( "cbgxml_list: %s" % cbgxml_list )
	
	add_all = False
	ghoe_type = ""
	
	if not cbgxml_list:
		yaml_skip = 0
		dir_list = os.listdir( cbgxml_dir )
		dir_list.sort()
		for filename in dir_list:
			if filename.startswith( '.' ):			# ignore hidden files
				continue
			ghoe_type, rmtype, archive_name = split_xml_fname( filename )
			if archive_name:
				if filename in cbgxml_skip:
					print( "Ignoring (as specified) %s" % filename )
					yaml_skip += 1
				else:
					cbgxml_list.append( filename )
		
		if yaml_skip > 0:
			print( "\nIgnoring %d files as specified in config" % yaml_skip )
		
		yn = input( "No XML files specified by the cbgxml_list \nAdd all %d eligible xml files from the cbgxml_dir? [y,N] "  % len( cbgxml_list ) )
		if yn.lower() == 'y':
			add_all = True
	
	subdir = os.path.basename(  cbgxml_dir )
	#timestamp = arrow.now().format( "YYYY.MM.DD-HH:mm" )
	timestamp = arrow.now().format( "YYYY.MM.DD" )
	sh_filename = "ingest-%s-%s.sh" % ( ghoe_type, timestamp )
	sh_pathname = os.path.join( os.path.dirname(__file__), sh_filename )
	print( "sh_pathname: %s" % sh_pathname )
	
	csv_filename = "ingest-%s-%s.csv" % ( ghoe_type, timestamp )
	csv_pathname = os.path.join( os.path.dirname(__file__), csv_filename )
	data = tablib.Dataset()
	
	encoding = "utf-8"
	newline  = '\n'
		
	with io.open( sh_pathname, "w", newline = newline, encoding = encoding ) as sh_file:
		# write header
		sh_file.write( "#!/bin/sh\n" )
		sh_file.write( "\n" )
		sh_file.write( "# Project LINKS, KNAW IISH\n" )
		sh_file.write( "# %s\n" % timestamp )
		sh_file.write( "\n" )
		sh_file.write( "# perl parameters:\n" )
		sh_file.write( "# [Perl File] [XML File] [db URL] [id_source] [registration_maintype] [drop-and-create] [db usr] [db pwd]\n" )
		sh_file.write( "# [drop-and-create]: first xml file:  1 = truncate a2a tables\n" )
		sh_file.write( "# [drop-and-create]: other xml files: 0 = keep a2a tables contents\n" )
		sh_file.write( "\n\n" )
		sh_file.write( "date \"+%F %T\"\n" )
		sh_file.write( "\n" )
		
		naccept  = 0
		nskipped = 0
		
		for f, xml_fname in enumerate( cbgxml_list ):
			print( "%d %s" % ( f+1, xml_fname ) )
			
			gho_type, rmtype, archive_name = split_xml_fname( xml_fname )
			if not archive_name:
				print( "Skipping %s" % xml_fname )
				nskipped += 1
				continue
			
			id_source, short_name = get_id_source_from_archive( db_ref, archive_name )
			if not id_source:
				data.append( [ f+1, 0, '',  archive_name ] )
				print( "Skipping %s\n" % xml_fname )
				nskipped += 1
				continue
			
			if add_all == False:
				yn = input( "Add %s? [y,N] " % xml_fname )
				if yn.lower() != 'y':
					nskipped += 1
					continue
			
			if naccept == 0:
				truncate = 1	# truncate a2a tables
			else:
				truncate = 0	# append to a2a tables
			
			naccept += 1
			xml_path = os.path.join( cbgxml_dir, xml_fname )
			
			perl_line = 'perl %s/import_a2a_auto.pl "%s" %s %d %d %d %s %s' % \
			( a2aperl_dir, xml_path, host_links, id_source, rmtype, truncate, user_links, passwd_links )
		
			if debug: print( perl_line )
			sh_file.write( perl_line + '\n' )
			
			data.append( [ f+1, int( id_source ), short_name,  archive_name ] )
		
		sh_file.write( "\ndate \"+%F %T\"\n" )
		sh_file.write( "\n# [eof]\n" )
		
		print( "\n%d filenames considered, of which:" % len( cbgxml_list ) )
		print( "%d filenames skipped" % nskipped )
		print( "%d filenames written to %s" % ( naccept, sh_filename ) )
	
		encoding  = "utf-8"
		delimiter = ';'
		newline   = '\n'
		data.headers = [ "#", "id_source", "short_name", "archive_name" ]
		data = data.sort( "id_source", reverse = False )
		with io.open( csv_pathname, "w", encoding = encoding ) as csv_file:
			quoting = csv.QUOTE_NONNUMERIC
			csv_file.write( data.export( "csv", delimiter = delimiter, quoting = quoting ) )
		
# process_xml()



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
	if debug: print( "mk_ingest_cbgxml.py" )
	
	time0 = time()		# seconds since the epoch
	
	yaml_filename = "./mk_ingest_cbgxml.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )
	
	A2APERL_DIR = config_local.get( "A2APERL_DIR", "./" )
	print( "A2APERL_DIR: %s" % A2APERL_DIR )
	
	CBGXML_COLLECTION = config_local.get( "CBGXML_COLLECTION", "" )
	print( "CBGXML_COLLECTION: %s" % CBGXML_COLLECTION )
	
	CBGXML_DIR_  = "CBGXML_%s_DIR"  % CBGXML_COLLECTION
	CBGXML_LIST_ = "CBGXML_%s_LIST" % CBGXML_COLLECTION
	CBGXML_SKIP_ = "CBGXML_%s_SKIP" % CBGXML_COLLECTION
	
	#print( "CBGXML_DIR_:  %s" % CBGXML_DIR_ )
	#print( "CBGXML_LIST_: %s" % CBGXML_LIST_ )
	#print( "CBGXML_SKIP_: %s" % CBGXML_SKIP_ )
	
	CBGXML_DIR  = config_local.get( CBGXML_DIR_, "./" )
	CBGXML_LIST = config_local.get( CBGXML_LIST_, [] )
	CBGXML_SKIP = config_local.get( CBGXML_SKIP_, [] )
	
	print( "CBGXML_DIR:  %s" % CBGXML_DIR )
	print( "CBGXML_LIST: %s" % CBGXML_LIST )
	print( "CBGXML_SKIP: %s" % CBGXML_SKIP )
	
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
	
	process_xml( db_ref, HOST_LINKS, USER_LINKS, PASSWD_LINKS, A2APERL_DIR, CBGXML_DIR, CBGXML_LIST, CBGXML_SKIP )

# [eof]
