#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		chk_cbgxml.py
Version:	0.2
Goal:		Check CBG XMl files:
			1) find max length of tag text
			1) get certificate counts of the xml files

18-Dec-2021 Created
24-May-2021 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import arrow
import csv
import datetime
import io
import os
import sys
import tablib
import yaml

from lxml import etree
from time import time

from a2a_xml import split_xml_fname, get_id_source_from_archive

debug = False


def collection_sizes( db_ref, cbgxml_dir ):
	print( "\ncollection_sizes()" )
	print( cbgxml_dir )
	
	tot_count = 0				# total number of registrations
	data = tablib.Dataset()		# collect data for csv file
	
	dir_list = os.listdir( cbgxml_dir )
	dir_list.sort()

	print( "number of registrations per XML file:" )
	for f, filename in enumerate( dir_list ):
		if filename.startswith( '.' ):				# ignore hidden files
			continue
		elif not filename.endswith( ".xml" ):		# ignore non-xml files
			continue
		
		ghoe_type, rmtype, archive_name = split_xml_fname( filename )
		
		if debug: print( filename )
		pathname = os.path.join( cbgxml_dir, filename )
		if debug: print( pathname )
		
		tree = etree.parse( pathname )
		root = tree.getroot()
		root_tag = etree.QName( root.tag ).localname
		if debug: print ( "root: %s" % root_tag )
		
		if root_tag == "A2ACollection":
			reg_count = len( root )
			print ( "%d: %s" % ( reg_count, filename ) )
			tot_count += reg_count
			
			gho_type, rmtype, archive_name = split_xml_fname( filename )
			id_source, short_name = get_id_source_from_archive( db_ref, archive_name )
			data.append( [ f+1, reg_count,  int( id_source ), short_name,  archive_name ] )

	# write counts data to csv file
	csv_filename = "BS-certificate-counts.csv"			# default
	# try to get BSG/BSH/BSE/BSO
	tail = ""
	( head, tail ) = os.path.split( cbgxml_dir )
	if "BS" in tail: 
		csv_filename = "%s.csv" % tail
	else:
		( head, tail ) = os.path.split( head )
		if "BS" in tail:
			csv_filename = "%s-certificate-counts.csv" % tail
	
	print( csv_filename )
	csv_pathname = os.path.join( os.path.dirname(__file__), csv_filename )

	encoding  = "utf-8"
	delimiter = ';'
	newline   = '\n'
	data.headers = [ "#", "registrations", "id_source", "short_name", "archive_name" ]
	data = data.sort( "id_source", reverse = False )
	
	coll_name = "%s total" % tail
	data.append( [ '', tot_count,  '-', '-',  coll_name ] )
	
	with io.open( csv_pathname, "w", encoding = encoding ) as csv_file:
		quoting = csv.QUOTE_NONNUMERIC
		csv_file.write( data.export( "csv", delimiter = delimiter, quoting = quoting ) )
		print( "written: %s" % csv_filename )
# collection_sizes()



def max_text_length( cbgxml_dir, tag_test ):
	print( "\nmax_text_length()" )
	print( cbgxml_dir )
	
	len_max  = 0
	text_max = ""
	
	dir_list = os.listdir( cbgxml_dir )
	dir_list.sort()
	
	for filename in dir_list:
		if filename.startswith( '.' ):			# ignore hidden files
			continue
		print( filename )
		pathname = os.path.join( cbgxml_dir, filename )
		if debug: print( pathname )
		
		tree = etree.parse( pathname )
		
		for tag in tree.iter():
			if not len( tag ):		# select only (end) leafs
				tag_name = etree.QName( tag ).localname
				#print ( tag_name )
				if tag_name == tag_test:
					tag_text = "%s" % tag.text
					len_text = len( tag_text )
					if len_text > len_max:
						print ( tag_name, len_text, tag_text )
						len_max  = len_text
						text_max = tag_text
			else:					# intermediate branch
				pass
	
	return len_max, text_max
# max_text_length()



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
	if debug: print( "chk_cbgxml.py" )
	
	time0 = time()		# seconds since the epoch
	
	yaml_filename = "./chk_cbgxml.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )
	
	main_dir = os.path.dirname( YAML_MAIN )
	sys.path.insert( 0, main_dir )
	from hsn_links_db import Database, format_secs
	
	CBGXML_BSG_DIR = config_local.get( "CBGXML_BSG_DIR" )
	CBGXML_BSH_DIR = config_local.get( "CBGXML_BSH_DIR" )
	CBGXML_BSE_DIR = config_local.get( "CBGXML_BSE_DIR" )
	CBGXML_BSO_DIR = config_local.get( "CBGXML_BSO_DIR" )
	
	CBGXML_DIR_LIST = [ CBGXML_BSG_DIR, CBGXML_BSH_DIR, CBGXML_BSE_DIR, CBGXML_BSO_DIR ]
	
	function = config_local.get( "FUNCTION" )
	
	if function == "TAG_TEXT_MAX_LENGTH":
		tag_test = config_local.get( "TAG_NAME" )
		print( "Find maximum text length of tag: %s" % tag_test )
		
		for CBGXML_DIR in CBGXML_DIR_LIST:
			len_max, text_max = max_text_length( CBGXML_DIR, tag_test )
			print( "maximum for %s" % CBGXML_DIR )
			print( "len_max: %s, text_max: %s" % ( len_max, text_max ) )
	
	elif function == "CERTIFICATE_COUNTS":
		print( "Get certificate counts of the cbg a2a xml files" )
		HOST_REF   = config_main.get( "HOST_REF" )
		USER_REF   = config_main.get( "USER_REF" )
		PASSWD_REF = config_main.get( "PASSWD_REF" )
		DBNAME_REF = config_main.get( "DBNAME_REF" )
		
		print( "HOST_REF: %s" % HOST_REF )
		print( "USER_REF: %s" % USER_REF )
		print( "PASSWD_REF: %s" % PASSWD_REF )
		print( "DBNAME_REF: %s" % DBNAME_REF )
		
		main_dir = os.path.dirname( YAML_MAIN )
		sys.path.insert( 0, main_dir )
		from hsn_links_db import Database, format_secs
		
		print( "Connecting to database at %s" % HOST_REF )
		db_ref = Database( host = HOST_REF,   user = USER_REF,   passwd = PASSWD_REF,   dbname = DBNAME_REF )
		
		for CBGXML_DIR in CBGXML_DIR_LIST:
			collection_sizes( db_ref, CBGXML_DIR )
	
	str_elapsed = format_secs( time() - time0 )
	print( "Done in %s\n" % str_elapsed )

# [eof]
