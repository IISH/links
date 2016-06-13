#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		a2a_to_original.py
Version:	0.3
Goal:		Read the sql file with MySQL queries to fill the 2 table of 
			links_original from the links_a2a tables. 
			Show progress by showing the queries one-by-one. 

04-Dec-2014	Created
18-Jan-2016	Update for Python-3
20-Feb-2016	Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import os
import sys
import datetime
from time import time
import MySQLdb

debug = False
keep_comments = False

# db
HOST   = "localhost"
USER   = "links"
PASSWD = "mslinks"
DBNAME = "links_a2a"

sql_dirname  = os.path.dirname( os.path.realpath( __file__ ) )
sql_filename = "a2a_to_original.sql"
sql_path     = os.path.join( sql_dirname, sql_filename )


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
		print( "\n%s" % query )
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



def queries( log ):
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
			log.write( "query %d-of-%d ...\n" % ( nq, nqueries ) )
			log.write( "%s\n" % query )
			log.flush()

			resp = db.query( query )
			if resp is not None: 
				log.write( "%s\n" % query )
				log.write( "%s\n" % str( resp ) )
				print( resp )

			info = db.info()
			if info is not None: 
				log.write( "%s\n" % str( info ) )

			str_elapsed = format_secs( time() - t1 )
			log.write( "query in %s\n\n" % str_elapsed )
			log.flush()



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
	ts = time()
	tsf = datetime.datetime.fromtimestamp( ts ).strftime( "%Y-%m-%d_%H:%M:%S" )

	cur_dirname = os.path.dirname( os.path.realpath( __file__ ) )
	log_dirname = os.path.join( cur_dirname, "log" )

	# ensure the log directory exists
	if not os.path.exists( log_dirname  ):
		os.makedirs( log_dirname  )

	log_filename = "A2O-%s.log" % tsf
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
	db = Database( host = HOST, user = USER, passwd = PASSWD, dbname = DBNAME )

	db_check( db )

	queries( log )
	str_elapsed = format_secs( time() - t1 )
	log.write( "Done in %s\n" % str_elapsed )

# [eof]
