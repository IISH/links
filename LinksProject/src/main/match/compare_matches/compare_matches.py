#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		compare_matches.py
Version:	0.1
Goal:		create a temp table to compare matches from given id_match_process values

16-Oct-2017 Created
17-Oct-2017 Changed
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
import re
import yaml


debug = False

# db settings, read values from config file
HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""


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



def get_table_names( db ):
	if debug: print( "get_table_names()" )
	
	query = "USE links_temp;"
	if debug: 
		print( "\n%s" % query )

	resp = db.query( query )
	if resp is not None and len( resp ) != 0:
		if debug: print( resp )

	query = "SHOW TABLES;"
	if debug: 
		print( "\n%s" % query )

	table_names = []
	resp = db.query( query )
	if resp is not None and len( resp ) != 0:
		if debug: print( resp )
		for entry in resp:
			table_name = entry[ "Tables_in_links_temp" ]
			if debug: print( table_name )
			table_names.append( table_name  )

	return table_names



def create_table_name( db, mpids, check_mpids ):
	if debug: print( "create_table_name()" )
	
	# get info for these ids
	use_query = "USE links_match;"
	if debug: 
		print( "\n%s" % use_query )
	
	use_resp = db.query( use_query )
	if use_resp is not None and len( use_resp ) != 0:
		print( use_resp )

	select_query = "SELECT * FROM match_process"
	for i, mpid in enumerate( mpids ):
		#print( "%d: %s" % ( i, mpid ) )
		if i == 0:
			select_query += " WHERE id = %s" % mpid
		else:
			select_query += " OR id = %s" % mpid
	select_query += ";"
	print( "\n%s" % select_query )
	
	s1_source = None
	s2_source = None
	
	s1_maintype  = None
	s2_maintype  = None
	
	select_resp = db.query( select_query )
	if select_resp is not None:
		nrecs = len( select_resp )
		#print( resp )
		if nrecs == 0:
			print( "No valid ids found" )
			return
		else:
			# sources and maintypes shoud not vary
			for i, rec in enumerate( select_resp ):
				#print( "%d: %s" % ( i, rec ) )
				if i == 0:
					s1_source = rec[ "s1_source" ]
					s2_source = rec[ "s2_source" ]
					
					s1_maintype  = rec[ "s1_maintype" ]
					s2_maintype  = rec[ "s2_maintype" ]
				else:
					if check_mpids:
						if s1_source != rec[ "s1_source" ]:
							print( "s1_source: %s %s" % ( s1_source, rec[ "s1_source" ] ) )
							print( "varying s1_source not admitted.\nEXIT." )
							exit( 0 )
						if s2_source != rec[ "s2_source" ]:
							print( "s2_source: %s %s" % ( s2_source, rec[ "s2_source" ] ) )
							print( "varying s2_source not admitted.\nEXIT." )
							exit( 0 )
						if s1_maintype != rec[ "s1_maintype" ]:
							print( "s1_maintype: %s %s" % ( s1_maintype, rec[ "s1_maintype" ] ) )
							print( "varying s1_maintype not admitted.\nEXIT." )
							exit( 0 )
						if s2_maintype != rec[ "s2_maintype" ]:
							print( "s2_maintype: %s %s" % ( s2_maintype, rec[ "s2_maintype" ] ) )
							print( "varying s2_maintype not admitted.\nEXIT." )
							exit( 0 )
	
	if not s1_source:
		s1_source = "all"
	if not s2_source:
		s2_source = "all"
	
	print( "s1_source: %s, s1_maintype: %s" % ( s1_source, s1_maintype ) )
	print( "s2_source: %s, s2_maintype: %s" % ( s2_source, s2_maintype ) )
	table_name = "matches_s1_%s_%s_s2_%s_%s" % ( s1_source, s1_maintype, s2_source, s2_maintype )
	print( "table name: %s" % table_name )
	
	return table_name



def drop_table( table_name ):
	if debug: print( "drop_table()" )
	
	query = "USE links_temp;"
	if debug: 
		print( "\n%s" % query )

	resp = db.query( query )
	if resp is not None and len( resp ) != 0:
		print( resp )

	query = "DROP TABLE links_temp.`%s`;" % table_name
	print( "\n%s" % query )

	resp = db.query( query )
	if resp is not None and len( resp ) != 0:
		print( resp )



def create_table( db, mpids, table_name ):
	if debug: print( "create_table()" )
	
	create_query = "CREATE TABLE links_temp.`%s` (\n" % table_name
	create_query += "id INT UNSIGNED NOT NULL AUTO_INCREMENT, \n"
	create_query += "id_linksbase_1 INT(10) UNSIGNED DEFAULT NULL, \n"
	create_query += "id_linksbase_2 INT(10) UNSIGNED DEFAULT NULL, \n"
	
	for mpid in mpids:
		create_query += "mpid_%s TINYINT UNSIGNED DEFAULT 0, \n" % mpid
	
	create_query += "PRIMARY KEY (`id`), \n"
	create_query += "UNIQUE KEY `id_linksbase` (`id_linksbase_1`, `id_linksbase_2`) \n"
	
	create_query += ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
	print( "\n%s" % create_query  )
	
	create_resp = db.query( create_query )
	if create_resp is not None and len( create_resp ) != 0:
		print( create_resp )



def fill_table( db, mpids, table_name ):
	if debug: print( "fill_table()" )
	
	# copy the s1 & s2 links_base ids
	# IGNORE to prevent duplicate ( id_linksbase_1, id_linksbase_2 ) combinations
	insert_query  = "INSERT IGNORE INTO links_temp.`%s`" % table_name
	insert_query += "( id_linksbase_1, id_linksbase_2 ) \n"
	insert_query += "SELECT id_linksbase_1, id_linksbase_2 FROM links_match.matches \n"
	
	for i, mpid in enumerate( mpids ):
		#print( "%d: %s" % ( i, mpid ) )
		if i == 0:
			insert_query += "WHERE"
		else:
			insert_query += " OR"
		insert_query += " id_match_process = %s" % mpid
	insert_query += ";"
	
	print( "\n%s" % insert_query  )
	insert_resp = db.insert( insert_query )
	if insert_resp is not None:
		print( "%d records inserted in table %s" % ( insert_resp, table_name ) )

	# update the mpid columns
	for i, mpid in enumerate( mpids ):
		#print( "%d: %s" % ( i, mpid ) )
		update_query  = "UPDATE links_temp.`%s`, links_match.matches \n" % table_name
		update_query += "SET mpid_%s = 1 \n" % mpid
		update_query += "WHERE `%s`.id_linksbase_1 = matches.id_linksbase_1 \n" % table_name
		update_query += "  AND `%s`.id_linksbase_2 = matches.id_linksbase_2 \n" % table_name
		update_query += "AND matches.id_match_process = %s;" % mpid
		
		print( "\n%s" % update_query  )
		update_resp = db.update( update_query )
		if update_resp is not None:
			print( "%d records updated in table %s" % ( update_resp, table_name ) )



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
	if debug: print( "compare_matches.py" )
	
	time0 = time()		# seconds since the epoch
	
	config_path = os.path.join( os.getcwd(), "compare_matches.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	db = Database( host = HOST_LINKS , user = USER_LINKS , passwd = PASSWD_LINKS , dbname = DBNAME_LINKS )
	
	mpids = []
	prompt = True
#	prompt = False
	if prompt:
		print( "Creating a comparison table for match_process ids." )
		print( "Please provide a list of match_process ids that you want to compare: " )
		mpids_str = input( "mpids: " )		# e.g: 322 323, 324
		mpids = re.split( "[,; ]+", mpids_str )
		
		print( "The list is: %s" % mpids )
		yn = input( "Continue? [n,Y] " )
		if yn.lower() == 'n':
			exit( 0 )
	else:
		mpids = ['322', '324']				# NB
		#mpids = ['330', '331']				# NB
		#mpids = ['5', '6', '7', '8']		# node-154
	
	check_mpids = False
#	check_mpids = True	# check sources and maintypes: they should not vary
	table_name = create_table_name( db, mpids, check_mpids )
	
	while True:
		table_names = get_table_names( db )		# table names in links_temp
		if table_name in table_names:
			print( "Table name %s already exists." % table_name )
			yn = input( "Drop existing table %s ? [N,y] " % table_name )
			if yn.lower() == 'y':
				drop_table( table_name )
			else:
				table_name = input( "New table name: " )
				if table_name == '':
					print( "Invalid table name '%s'.\nEXIT." % table_name )
					exit( 0 )
				
				print( "The new table name is: %s" % table_name )
				yn = input( "Continue? [n,Y] " )
				if yn.lower() == 'n':
					exit( 0 )
		else:
			break
	
	create_table( db, mpids, table_name)
	fill_table( db, mpids, table_name )
	
	str_elapsed = format_secs( time() - time0 )
	print( "\nprocessing took %s" % str_elapsed )
		
# [eof]
