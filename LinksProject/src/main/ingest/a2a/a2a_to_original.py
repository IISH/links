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
05-Jun-2018	Changed
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
	next, object, oct, open, pow, range, round, super, str, zip )

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
		affected_count = None
		try:
			affected_count = self.cursor.execute( query )
			self.connection.commit()
		except:
			self.connection.rollback()
			etype = sys.exc_info()[ 0:1 ]
			value = sys.exc_info()[ 1:2 ]
			print( "%s, %s\n" % ( etype, value ) )
		return affected_count
	
	def update( self, query ):
		return self.insert( query )
	
	def delete( self, query ):
		return self.insert( query )
	
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



def queries( db, log ):
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



def sources_from_a2a( db, log ):
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



def delete_from_orig( db, log, rmtypes ):
	log.write( "delete_from_orig()\n" )
	
	db_name = "links_original"
	
	for dict_src in rmtypes:
		source = int( dict_src[ "source" ] )
		rmtype = int( dict_src[ "rmtype" ] )
		
		table = "registration_o"
		query_r  = "DELETE FROM %s.%s " % ( db_name, table )
		query_r += "WHERE id_source = %d AND registration_maintype = %d;" % ( source, rmtype )
		log.write( "%s\n" % query_r )
		count = db.delete( query_r )
		log.write( "%d records deleted\n" % count )
		
		table = "person_o"
		query_p  = "DELETE FROM %s.%s " % ( db_name, table )
		query_p += "WHERE id_source = %d AND registration_maintype = %d;" % ( source, rmtype )
		log.write( "%s\n" % query_p )
		count = db.delete( query_p )
		log.write( "%d records deleted\n" % count )



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
	
	config_path = os.path.join( os.getcwd(), "a2a_to_original.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )

#	db_check( db_links )

	rmtypes = sources_from_a2a( db_links, log )
	delete_from_orig( db_links, log, rmtypes )

	queries( db_links, log )		# queries from existing sql file
	
	str_elapsed = format_secs( time() - t1 )
	log.write( "Done in %s\n" % str_elapsed )

# [eof]
