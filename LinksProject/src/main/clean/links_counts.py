#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		links_counts.py
Version:	0.1
Goal:		Count and compare links original & cleaned record counts

29-Mar-2016 Created
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
import yaml

debug = True

# db settings, read values from config file
HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""


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
	"238" : "Z-H_Midden",
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
	query_id = "query_o"
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
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		sources[ id_source ] = [ entry ]	# clean counts dict will be added to array
	
	if debug: print( sources )
	
	
	# get the number of registration records per source from links_cleaned
	table = "registration_c"
	query_id = "query_c"
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
		entry[ "table" ] = table
		entry[ "query" ] = query_id
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
	print( "============================================================================" )
	print( "     id        short        registration_o  registration_c  --- o/c loss ---" )
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
	query_id = "query_o"
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
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		sources[ id_source ] = [ entry ]	# clean counts dict will be added to array
	
	if debug: print( sources )
	
	
	# get the number of person records per source from links_cleaned
	table = "person_c"
	query_id = "query_c"
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
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
		
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
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
	print( "============================================================================" )
	print( "     id        short           person_o        person_c     --- o/c loss ---" )
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
		
		diff_oc = orig - clean
		if orig != 0:
			procent_oc = 100.0 * float( diff_oc ) / ( orig )
			procent_oc_str = "%6.2f %%" % procent_oc
		else:
			procent_oc_str = "     "
		
		diff_cb = clean - base
		if clean != 0:
			procent_cb = 100.0 * float( diff_cb ) / ( clean )
			procent_cb_str = "%6.2f %%" % procent_cb
		else:
			procent_cb_str = "     "
		
		print( "%2d %4s   %-20s %7d         %7d   %7d    %5s" % 
			( n, id_source_str, name, orig, clean, diff_oc, procent_oc_str ) )
			
	print( "============================================================================\n" )

	return sources



def base_counts( debug, db, sources_person ):
	if debug: print( "base_counts" )
	
	sources_base = {}

	
	# Get the number of records per source from links_base.  
	# different counts between person_c and links_base are caused role and/or registration_maintype problems. 
	# A too large difference indicates a potential problem with cleaning and/or prematching. 
	table = "links_base"
	query_id = "query_b1"
	query_b1  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_b1 += " GROUP BY id_source" + ";"
	print( query_b1 )
	
	resp_b1 = db.query( query_b1 )
	if debug: print( resp_b1 )
	nsources_b1 = len( resp_b1 )
	if debug: print( "links_base: %d sources\n" % nsources_b1 )
	
	for s_b1 in range( nsources_b1 ):
		source = {}
		d = resp_b1[ s_b1 ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )

	
	# Get the number of records per source from links_base, 
	# where the ego_familyname is missing; these are not used for matching. 
	table = "links_base"
	query_id = "query_be"
	query_be  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_be += " WHERE ego_familyname IS NULL OR ego_familyname = 0"
	query_be += " GROUP BY id_source" + ";"
	print( query_be )
	
	resp_be = db.query( query_be )
	if debug: print( resp_be )
	nsources_be = len( resp_be )
	if debug: print( "links_base: %d sources\n" % nsources_be )
	
	for s_be in range( nsources_be ):
		source = {}
		d = resp_be[ s_be ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )
	
	
	# Get the number of records per source from links_base, 
	# where the registration_days is missing; these are not used for matching. 
	table = "links_base"
	query_id = "query_bd"
	query_bd  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_bd += " WHERE registration_days IS NULL OR registration_days = 0"
	query_bd += " GROUP BY id_source" + ";"
	print( query_bd )
	
	resp_bd = db.query( query_bd )
	if debug: print( resp_bd )
	nsources_bd = len( resp_bd )
	if debug: print( "links_base: %d sources\n" % nsources_bd )

	for s_bd in range( nsources_bd ):
		source = {}
		d = resp_bd[ s_bd ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
		#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )


	# Get the number of records per source from links_base. 
	# The requirements for ego_familyname and registration_days may lead to 
	# different counts between person_c and links_base. 
	# A too large difference indicates a potential problem with cleaning and/or prematching. 
	table = "links_base"
	query_id = "query_b2"
	query_b2  = "SELECT id_source, COUNT(*) AS count FROM links_prematch." + table
	query_b2 += " WHERE ego_familyname <> 0 AND registration_days IS NOT NULL AND registration_days <> 0"
	query_b2 += " GROUP BY id_source" + ";"
	print( query_b2 )

	resp_b2 = db.query( query_b2 )
	if debug: print( resp_b2 )
	nsources_b2 = len( resp_b2 )
	if debug: print( "links_base: %d sources\n" % nsources_b2 )

	for s_b2 in range( nsources_b2 ):
		source = {}
		d = resp_b2[ s_b2 ]
		id_source = d.get( "id_source" )
		count     = d.get( "count" )
		source[ "base" ] = count
	#	#print( "# %3d id_source: %d, count: %d" % ( s+1, id_source, count ) )
	
		entry = {}
		entry[ "table" ] = table
		entry[ "query" ] = query_id
		entry[ "count" ] = count
		
		array = sources_base.get( id_source )
		if array is None:
			sources_base[ id_source ] = [ entry ]
		else:
			array.append( entry )
	
	if debug: print( sources_base )


	now = datetime.datetime.now()
	print( "" )
	print( "host: %s, date: %s" % ( HOST, str( now.strftime( "%Y-%m-%d" ) ) ) )
	print( "============================================================================================" )
	print( "     id        short         links_base   -- c/b loss --  missing  missing  -- b/b loss --" )
	print( " # source  archive name     record count  diff   procent  ego fam  days sb  diff   procent" )
	print( "--------------------------------------------------------------------------------------------" )
		
	n = 0
	for key in sorted( sources_base ):
		n += 1
		
		tables_person = sources_person[ key ]
		count_c = 0
		for t in tables_person:
			t_name = t.get( "table" )
			if t_name == "person_c":
				count_c = t.get( "count" )
		clean = count_c
		
		queries_base = sources_base[ key ]
		#print( key, queries_base )
		
		id_source = key
		id_source_str = str( id_source )
		
		try:
			name = archive_names[ id_source_str ]
		except:
			name = ""
		
		count_b1 = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_b1":
				count_b1 = q.get( "count" )
		base1 = count_b1
		
		diff_cb = clean - base1
		if clean != 0:
			procent_cb = 100.0 * float( diff_cb ) / ( clean )
			procent_cb_str = "%6.2f %%" % procent_cb
		else:
			procent_cb_str = "     "
		
		count_be = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_be":
				count_be = q.get( "count" )
		ego = count_be
		
		count_bd = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_bd":
				count_bd = q.get( "count" )
		days = count_bd
		
		count_b2 = 0
		for q in queries_base:
			q_id = q.get( "query" )
			if q_id == "query_b2":
				count_b2 = q.get( "count" )
		base2 = count_b2
		
		# wrong: diff_bb = ego + days, because ego and days may contain overlapping records
		diff_bb = base1 - base2
		if base1 != 0:
			procent_bb = 100.0 * float( diff_bb ) / ( base1 )
			procent_bb_str = "%6.2f %%" % procent_bb
		else:
			procent_bb_str = "     "
		
		print( "%2d %4s   %-20s %7d %7d  %5s %7d %7d %7d  %5s" % 
			( n, id_source_str, name, base1, diff_cb, procent_cb_str, ego, days, diff_bb, procent_bb_str ) )

	print( "============================================================================================\n" )



if __name__ == "__main__":
	config_path = os.path.join( os.getcwd(), "links_counts.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	db = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
#	db_check( db )

	print( "host:", HOST )
	registration_counts( debug, db )
	sources_person = person_counts( debug, db )
	base_counts( debug, db, sources_person )

# [eof]