#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		hsn-links-db.py
Version:	0.2
Goal:		db access

25-Nov-2020 Created
05-May-2021 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import sys

import MySQLdb

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
			charset = "utf8",				# needed when there is e.g. 
			use_unicode = True				# &...; html escape stuff in strings
		)
		self.cursor = self.connection.cursor()


	def execute( self, query ):
		try:
			resp = self.cursor.execute( query )
			self.connection.commit()
			return resp
		except:
			self.connection.rollback()
			etype = sys.exc_info()[ 0:1 ]
			value = sys.exc_info()[ 1:2 ]
			print( "%s, %s\n" % ( etype, value ) )

	def insert( self, query ):
		return self.execute( query )

	def update( self, query ):
		return self.execute( query )

	def delete( self, query ):
		return self.execute( query )

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
# Database



def get_archive_name( db_ref, id_source ):
	if debug: print( "get_archive()" )

	source_name = "id_source=%s" % id_source
	short_name  = "id_source=%s" % id_source
	
	query = "SELECT source_name, short_name FROM ref_source WHERE id_source = %s" % id_source
	if debug: print( query )
	resp = db_ref.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if nrec == 0:
			print( "No valid id_source record found in ref_source for id_source = %s" % id_source )
		elif nrec == 1:
			rec = resp[ 0 ]
			source_name = rec[ "source_name" ]
			short_name  = rec[ "short_name" ]
			
			if not source_name:
				source_name = "id_source=%s" % id_source
			if not short_name:
				short_name = "id_source=%s" % id_source
		else:
			print( "Too many id_source records found, ignoring them all" )
	
	if debug:
		print( "source_name = %s, short_name = %s" % ( source_name, short_name ) )
	
	return source_name, short_name
# get_archive_name()



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



def none2empty( var ):
	if var is None or var == "None" or var == "null":
		var = ""
	return var
# none2empty()



def none2zero( var ):
	ivar = 0
	try:
		ivar = int( var )
	except:
		ivar = 0
		
	return ivar
# none2zero()

# [eof]
