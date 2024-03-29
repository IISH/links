#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW HuC-DI & IISH - International Institute of Social History
Project:	LINKS
Name:		export_csv.py
Version:	0.3
Goal:		Export a table, including header, to a csv file
ToDo:		Move general db stuff to a separate hsn-links-db.py
			Get db & tabl name from command line or yaml file

08-May-2020 Created
18-Sep-2020 2 yaml files: export_csv & hsn-links-db
21-Sep-2020 quotechar  = '|' to avoid problems with the common " and ' in strings
30-Sep-2020 WHERE string also in COUNT(*)
01-Mar-2021 python version dependent csv import
30-Jun-2021 clean whitespace query string
"""

# future imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

#import csv
import datetime
import MySQLdb
import io
import os
import sys
import yaml

from time import time

if sys.version_info[ 0 ] == 2:		# Python-2
	from backports import csv		# better csv support for Python-2
else:
	import csv

# CSV creation parameters: 
encoding   = "utf-8"
newline    =  '\n'
delimiter  = ';'					# ','
escapechar = '\\'
quotechar  = '"'					# '|' use for links_original
quoting    = csv.QUOTE_NONNUMERIC	# quote strings, not numbers

debug = False


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



def export( debug, db, table_name, columns_str, where_str, csv_filename ):
	if not csv_filename:
		csv_filename = "%s.csv" % table_name
	
	csv_pathname = os.path.join( os.path.dirname(__file__), 'csv', csv_filename )
	print( "csv_pathname: %s" % csv_pathname )
	if not os.path.exists( os.path.dirname( csv_pathname ) ):
		try:
			os.makedirs( os.path.dirname( csv_pathname ) )
		except: 
			raise
	
	with io.open( csv_pathname, "w", newline = newline, encoding = encoding ) as csv_file:
		writer = csv.writer( csv_file, delimiter = delimiter, escapechar = escapechar, quotechar = quotechar, quoting = quoting )
		
		header = []
		if columns_str:
			column_names = columns_str.split( ',' )
			for col_name in column_names:
				header.append( col_name.strip() )
		else:
			query = "SHOW COLUMNS FROM %s" % table_name
			print( "query: %s" % query )
			resp = db.query( query )
			
			if resp is not None:
				nrec = len( resp )
				print( "number of columns: %d" %nrec )
				for r in range( nrec ):
					col_dict = resp[ r ]
					col_name = col_dict[ "Field" ]
					header.append( col_name )
		
		print( header )
		writer.writerow( header )

		query = "SELECT COUNT(*) AS count FROM %s" % table_name
		if where_str:
			query = "%s WHERE %s" % ( query, where_str )
		print( "query: %s" % query )
		resp = db.query( query )
		if resp is not None:
			row = resp[ 0 ]
			nrec = row[ "count" ]
			print( "number of rows: %d" %nrec )

		if columns_str:
			query = "SELECT %s FROM %s" % ( columns_str, table_name )
		else:
			query = "SELECT * FROM %s" % table_name
			
		if where_str:
			query = "%s WHERE %s" % ( query, where_str )
		
		query = ' '.join( query.split() )		# clean whitespace
		print( "query: %s" % query )

		cursor = db.cursor
		cursor.execute( query )
		resp = cursor.fetchall()
		for row in resp:
			#print( row )
			writer.writerow( row )



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



if __name__ == "__main__":
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	
	python_vertuple = sys.version_info
	python_version = str( python_vertuple[ 0 ] ) + '.' + str( python_vertuple[ 1 ] ) + '.' + str( python_vertuple[ 2 ] )
	print( "Python version: %s" % python_version )
	
	print( "export_csv.py" )
	
	yaml_filename = "./export_csv.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	YAML_MAIN  = config_local.get( "YAML_MAIN" )
	DB_NAME    = config_local.get( "DB_NAME" )
	TABLE_NAME = config_local.get( "TABLE_NAME" )
	
	COLUMN_NAMES    = config_local.get( "COLUMN_NAMES" )
	WHERE_CONDITION = config_local.get( "WHERE_CONDITION" )
	CSV_FILENAME    = config_local.get( "CSV_FILENAME" )
	
	config_main = get_yaml_config( YAML_MAIN )
	
	HOST_LINKS   = config_main.get( "HOST_LINKS" )
	USER_LINKS   = config_main.get( "USER_LINKS" )
	PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	print( "USER_LINKS: %s" % USER_LINKS )
	print( "PASSWD_LINKS: %s" % PASSWD_LINKS )

	db = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DB_NAME )
	
	export( debug, db, TABLE_NAME, COLUMN_NAMES, WHERE_CONDITION, CSV_FILENAME )

	msg = "Stop: %s" % datetime.datetime.now()
	
	str_elapsed = format_secs( time() - time0 )
	print( "Creating csv took %s" % str_elapsed )
	
# [eof]
