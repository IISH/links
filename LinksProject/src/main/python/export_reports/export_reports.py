#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_reports.py
Version:	0.1
Goal:		

05-Aug-2016 Created
05-Aug-2016 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import os
import sys
import datetime
from time import time
from dateutil.parser import parse
import MySQLdb
#from collections import Counter

debug = True

#begin_date_default = "2015-01-01"
#end_date_default   = "2016-12-31"
begin_date_default = "2016-04-15"
end_date_default   = "2016-05-12"

# db
HOST   = "localhost"
#HOST   = "10.24.64.154"
#HOST   = "10.24.64.158"

USER   = "links"
PASSWD = "mslinks"
DBNAME = ""				# be explicit in all queries

"""
HOST_REF   = "10.24.64.30"
USER_REF   = "hsnref"
PASSWD_REF = "refhsn"
DBNAME_REF = ""				# be explicit in all queries
"""
HOST_REF   = "localhost"
USER_REF   = "links"
PASSWD_REF = "mslinks"
DBNAME_REF = ""				# be explicit in all queries

single_quote = "'"
double_quote = '"'

	
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
			log.write( "%s, %s\n" % ( etype, value ) )
			exit( 1 )

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
	
	
	return accept



def select_log_names( db_logs, begin_date, end_date ):
	if debug: print( "select_log_names()" )
	all_names = get_log_names( db_logs )
	log_names = []	# accepted log names
	nnames = len( all_names )
	
	for n in range( nnames ):
		log_name = all_names[ n ]
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



def process_logs( log_names ):
	print( "\nprocessing selected logs" )
	nnames = len( log_names )
	for n in range( nnames ):
		log_name = log_names[ n ]
		print( log_name )

		query = "SELECT * FROM links_logs.`%s`;" % log_name
		resp = db_logs.query( query )
		if resp is not None:
			ndict = len( resp )
			print( ndict, resp )



def validate_date( date_string ):
	try: 
		date = parse( date_string )
	except ValueError:
		print( "%s ValueError" % date_string )

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



if __name__ == "__main__":
#	print( "links host db: %s \tfor frequency and levenshtein tables" % HOST )
#	print( "reference db:  %s \tfor reference tables" % HOST_REF )

	print( "export_reports.py" )

	begin_date, end_date = get_date_limits()
	if begin_date is None or end_date is None:
		print( "EXIT" )
		sys.exit( 1 )
	
	db = Database( host = HOST, user = USER, passwd = PASSWD, dbname = DBNAME )

	db_ref = Database( host = HOST_REF, user = USER_REF, passwd = PASSWD_REF, dbname = DBNAME_REF )
	
	db_logs = Database( host = HOST, user = USER, passwd = PASSWD, dbname = DBNAME )
	
#	db_check( db )

	log_names = select_log_names( db_logs, begin_date, end_date )
	process_logs( log_names )
	

# [eof]
