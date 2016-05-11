#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		links_counts.py
Version:	0.1
Goal:		Count and compare links original & cleaned record counts

29-Mar-2016 Created
04-May-2016 Changed
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

# db
HOST   = "localhost"
#HOST   = "10.24.64.154"
#HOST   = "10.24.64.158"

USER   = "links"
PASSWD = "mslinks"
DBNAME = ""				# be explicit in all queries

"""
# incomplete
full_archive_names = [ 															# counts in 2016
	{ "id_source" : 211, "name" : "" },
	{ "id_source" : 212, "name" : "" },
	{ "id_source" : 213, "name" : "" },
	{ "id_source" : 214, "name" : "" },
	{ "id_source" : 215, "name" : "" },
	{ "id_source" : 216, "name" : "" },
	{ "id_source" : 217, "name" : "" },
	{ "id_source" : 218, "name" : "" },
	{ "id_source" : 219, "name" : "219-is-not-used" },
	{ "id_source" : 220, "name" : "" },
	{ "id_source" : 221, "name" : "" },
	{ "id_source" : 222, "name" : "" },
	{ "id_source" : 223, "name" : "" },
	{ "id_source" : 224, "name" : "" },
	{ "id_source" : 225, "name" : "" },
	{ "id_source" : 226, "name" : "" },
	{ "id_source" : 227, "name" : "" },
	{ "id_source" : 228, "name" : "" },
	{ "id_source" : 229, "name" : "" },
	{ "id_source" : 230, "name" : "" },
	{ "id_source" : 231, "name" : "Gemeentearchief Oegstgeest" },				# 21278 / ( 21278 + 837 ) = 0.96
#	{ "id_source" : 231, "name" : "Gemeentearchief Leidschendam-Voorburg" },	#   837 / ( 21278 + 837 ) = 0.04
	{ "id_source" : 232, "name" : "" },
	{ "id_source" : 233, "name" : "" },
	{ "id_source" : 234, "name" : "" },
	{ "id_source" : 235, "name" : "" },
	{ "id_source" : 236, "name" : "" },
	{ "id_source" : 237, "name" : "" },
	{ "id_source" : 238, "name" : "" },
	{ "id_source" : 239, "name" : "" },
	{ "id_source" : 240, "name" : "" },
	{ "id_source" : 241, "name" : "" },
	{ "id_source" : 242, "name" : "Gemeentearchief Wassenaar" },				# 29598 = 1.00
	{ "id_source" : 243, "name" : "Regionaal Archief Leiden" },
	{ "id_source" : 244, "name" : "" }
]
"""

short_archive_names = {
	"211" : "Groningen",
	"212" : "Fri_Tresoar",
	"213" : "Drenthe",
	"214" : "Overijssel",
	"215" : "Gelderland",
	"216" : "Utrecht",
	"217" : "N-H_Haarlem",
	"218" : "Z-H_Nat-Archief",
	"219" : "219-is-not-used",
	"220" : "NBr_BHIC",
	"221" : "Limburg",
	"222" : "Flevoland",
	"223" : "Z-H_Rotterdam",
	"224" : "NBr_Breda",
	"225" : "Zeeland",
	"226" : "NBr_Eindhoven",
	"227" : "Utr_Eemland",
	"228" : "Fri_Leeuwarden",
	"229" : "N-H_Alkmaar",
	"230" : "Ned-Antillen",
	"231" : "Z-H_Oegstgeest",
	"232" : "Z-H_Dordrecht",
	"233" : "Z-H_Voorne",
	"234" : "Z-H_Goeree",
	"235" : "Z-H_Rijnstreek",
	"236" : "Z-H_Midden-Holland",
	"237" : "Z-H_Vlaardingen",
	"238" : "Z-H-Midden",
	"239" : "Z-H_Gorinchem",
	"240" : "Z-H_Westland",
	"241" : "Z-H_Leidschendam",
	"242" : "Z-H_Wassenaar",
	"243" : "Z-H_Leiden",
	"244" : "Z-H_Delft"
}

archive_names = short_archive_names

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
	print( "db_check()" )

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



def registration_counts( debug, db ):
	if debug: print( "registration_counts" )
	
	sources = {}
	
	# get the number of registration records per source from links_original
	table = "registration_o"
	query_o = "SELECT id_source, COUNT(*) AS count FROM links_original." + table + " GROUP BY id_source" + ";"
	print( query_o )
	
	resp_o = db.query( query_o )
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
		entry[ "table" ]     = table
		entry[ "count" ]     = count
		
		sources[ id_source ] = [ entry ]	# clean counts dict will be added to array
	
	if debug: print( sources )
	
	
	# get the number of registration records per source from links_cleaned
	table = "registration_c"
	query_c = "SELECT id_source, COUNT(*) AS count FROM links_cleaned." + table + " GROUP BY id_source" + ";"
	print( query_c )
	
	resp_c = db.query( query_c )
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
		entry[ "table" ]     = table
		entry[ "count" ]     = count
		
		array = sources.get( id_source )
		if array is None:
			sources[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources )
	
	now = datetime.datetime.now()
	print( "" )
	print( "host: %s, date: %s" % ( HOST, str( now.strftime( "%Y-%m-%d" ) ) ) )
	print( "============================================================================" )
	print( "     id        short        registration_o  registration_c  -- difference --" )
	print( " # source  archive name      record count    record count   diff     procent" )
	print( "----------------------------------------------------------------------------" )
	
	n = 0
	for key in sorted( sources ):
		n += 1
		tables = sources[ key ]
		#print( key, tables )
		
		id_source = key
		id_source_str = str( id_source )
		
		try:
			name = archive_names[ id_source_str ]
		except:
			name = ""
			
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



def person_counts( debug, db ):
	if debug: print( "person_counts" )
	
	sources = {}
	
	# get the number of person records per source from links_original
	table = "person_o"
	query_o = "SELECT id_source, COUNT(*) AS count FROM links_original." + table + " GROUP BY id_source" + ";"
	print( query_o )
	
	resp_o = db.query( query_o )
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
		entry[ "table" ]     = table
		entry[ "count" ]     = count
		
		sources[ id_source ] = [ entry ]	# clean counts dict will be added to array
	
	if debug: print( sources )
	
	
	# get the number of person records per source from links_cleaned
	table = "person_c"
	query_c = "SELECT id_source, COUNT(*) AS count FROM links_cleaned." + table + " GROUP BY id_source" + ";"
	print( query_c )
	
	resp_c = db.query( query_c )
	if debug: print( resp_c )
	nsources_c = len( resp_c )
	if debug: print( "person_c: %d sources\n" % nsources_c )
	
	for s_c in range( nsources_c ):
		source = {}
		d = resp_c[ s_c ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "clean" ] = count
	#	#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
		
		entry = {}
		entry[ "table" ] = table
		entry[ "count" ] = count
		
		array = sources.get( id_source )
		if array is None:
			sources[ id_source ] = [ entry ]
		else:
			array.append( entry )
		
	if debug: print( sources )
	
	
	# Get the number of records per source from links_base. 
	# We add the requirement that the ego_familyname are non-zero, because 
	# 0-values mean that the prematch names-to-numbers has not been run, 
	# which makes the base table unusable for matching. 
	table = "links_base"
	query_b = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table + " WHERE ego_familyname <> 0 GROUP BY id_source" + ";"
	print( query_b )
	
	resp_b = db.query( query_b )
	if debug: print( resp_b )
	nsources_b = len( resp_b )
	if debug: print( "links_base: %d sources\n" % nsources_b )
	
	for s_b in range( nsources_b ):
		source = {}
		d = resp_b[ s_b ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
	#	#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "count" ] = count
		
		array = sources.get( id_source )
		if array is None:
			sources[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources )

	
	now = datetime.datetime.now()
	print( "" )
	print( "host: %s, date: %s" % ( HOST, str( now.strftime( "%Y-%m-%d" ) ) ) )
	print( "================================================================================================" )
	print( "     id        short           person_o        person_c      o/c difference    links_base   c/b " )
	print( " # source  archive name      record count    record count   diff     procent  record count  diff" )
	print( "------------------------------------------------------------------------------------------------" )
	
	n = 0
	for key in sorted( sources ):
		n += 1
		tables = sources[ key ]
		#print( key, tables )
		
		id_source = key
		id_source_str = str( id_source )
		
		try:
			name = archive_names[ id_source_str ]
		except:
			name = ""
			
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
		
		diff = orig - clean
		if orig != 0:
			procent = 100.0 * float( diff ) / ( orig )
		else:
			procent = None
		
		diff2 = clean - base
		
		if procent is not None:
			print( "%2d %4s   %-20s %7d         %7d   %7d    %6.2f %%    %7d %7d" % 
				( n, id_source_str, name, orig, clean, diff, procent, base, diff2 ) )
		else:
			procent_str = "     "
			print( "%2d %4s   %-20s %7d         %7d   %7d    %5s    %7d %7d" % 
				( n, id_source_str, name, orig, clean, diff, procent_str, base, diff2 ) )
			
	print( "================================================================================================\n" )



if __name__ == "__main__":
	db = Database( host = HOST, user = USER, passwd = PASSWD, dbname = DBNAME )

#	db_check( db )

	print( "host:", HOST )
	registration_counts( debug, db )
	person_counts( debug, db )

# [eof]
