#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:     Fons Laan, KNAW IISH - International Institute of Social History
Project:    LINKS
Name:       check_matches.py
Version:    0.1
Goal:       Check matches for usability
			After creating a new links_base table, the links_base_1 & links_base_2 
            values in the matches tables have lost there meaning.
            This script does a Levenshtein check on ego_familyname to find the 
            the match_process table entries that are outdated. 
            
17-Apr-2018 Created
23-Apr-2018 Changed
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

import os
import sys
import datetime
import stringdist			# pip install StringDist
from time import time
import MySQLdb
import yaml


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



def get_id_match_process_list( db_links ):
	print( "get_id_match_process_list()" )
	
	query = "SELECT * FROM links_match.match_process ORDER BY id;"
	if debug: print( query )
	resp = db_links.query( query )
	
	return resp



def check_matches( db_links, id_mp, lvs_max ):
	print( "check_matches() id_mp: %s, lvs_max: %s" % ( id_mp, lvs_max ) )
	
	query  = "SELECT M.id_matches, M.id_linksbase_1, M.id_linksbase_2, "
	query += "X.id_base, Y.id_base, X.ego_familyname_str, Y.ego_familyname_str " 
	query += "FROM links_match.matches as M, "
	query += "links_prematch.links_base as X, "
	query += "links_prematch.links_base as Y "
	query += "WHERE M.id_match_process = %s " % id_mp
	query += "AND X.id_base = id_linksbase_1 "
	query += "AND Y.id_base = id_linksbase_2 "
	query += "ORDER BY id_matches LIMIT 5;"

	if debug: print( query )
	resp = db_links.query( query )
	
	if len( resp ) == 0:
		print( "No corresponding links_base records found for id_match_process %d" % id_mp )
	
	for rec in resp:
		#print( str( rec ) )
		id_matches           = rec[ "id_matches" ]
		id_linksbase_1       = rec[ "id_linksbase_1" ]
		id_linksbase_2       = rec[ "id_linksbase_2" ]
		X_id_base            = rec[ "id_base" ]
		Y_id_base            = rec[ "Y.id_base" ]
		X_ego_familyname_str = rec[ "ego_familyname_str" ]
		Y_ego_familyname_str = rec[ "Y.ego_familyname_str" ]

		lvs = stringdist.levenshtein( X_ego_familyname_str, Y_ego_familyname_str )
		msg = "OK "
		if lvs > lvs_max:
			msg = "ERR"
		
		print( "id_matches: %s, id_linksbase_1&2: %s, %s, lvsd: %2d: %s ego_familyname_str 1&2: %s, %s" % 
		( id_matches, id_linksbase_1, id_linksbase_2, lvs, msg, X_ego_familyname_str, Y_ego_familyname_str ) )

	print( "" )



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
# format_secs()



if __name__ == "__main__":
	print( "check_matches.py" )
	time0 = time()		# seconds since the epoch
	
	config_path = os.path.join( os.getcwd(), "check_matches.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )

	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	DBNAME_LINKS = config.get( "DBNAME_LINKS" )
	
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )

	id_mp_list = get_id_match_process_list( db_links )
	
	for i, id_mp_dict in enumerate( id_mp_list ):
		id_mp   = id_mp_dict[ "id" ]
		lvs_max = id_mp_dict[ "prematch_familyname_value" ]
		check_matches( db_links, id_mp, lvs_max )
		#if i > 3:
		#	break
	
	str_elapsed = format_secs( time() - time0 )
	print( "processing took %s" % str_elapsed )



# [eof]
