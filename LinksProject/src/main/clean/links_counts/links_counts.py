#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		links_counts.py
Version:	0.3
Goal:		Count and compare links original, cleaned and base record counts
TODO:		Read archive_names directly from HOST_REF db

29-Mar-2016 Created
17-Dec-2018 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import datetime
import MySQLdb
import os
import sys
import yaml

from time import time

debug = False

# settings, read from config file
HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""

HOST_REF   = ""
USER_REF   = ""
PASSWD_REF = ""
DBNAME_REF = ""

# SELECT id_source, source_name, short_name FROM links_general.ref_source WHERE source_type = "BS" AND WWW_CollID IS NOT NULL AND id_source > 201 ORDER BY id_source;
long_archive_names = {
	"211" : "Groninger Archieven",
	"213" : "Drents Archief",
	"214" : "Historisch Centrum Overijssel",
	"215" : "Gelders Archief",
	"216" : "Het Utrechts Archief",
	"217" : "Noord-Hollands Archief",
	"218" : "Rijksarchief Zuid-Holland",
	"219" : "Alle Friezen",
	"220" : "Brabants Historisch Informatie Centrum",
	"221" : "Regionaal Historisch Centrum Limburg",
	"222" : "Het Flevolands Archief",
	"223" : "Stadsarchief Rotterdam",
	"224" : "Stadsarchief Breda",
	"225" : "Zeeuws Archief",
	"226" : "Regionaal Archief Eindhoven",
	"229" : "Regionaal Archief Alkmaar",
	"230" : "Nederlandse Antillen",
	"231" : "Gemeentearchief Oegstgeest",
	"232" : "Regionaal Archief Dordrecht",
	"233" : "Streekarchief Voorne-Putten en Rozenburg",
	"234" : "Streekarchief Goeree-Overflakkee",
	"235" : "Streekarchief Rijnstreek",
	"236" : "Streekarchief Midden-Holland",
	"237" : "Stadsarchief Vlaardingen",
	"238" : "Streekarchief Rijnlands Midden",
	"239" : "Regionaal Archief Gorinchem",
	"240" : "Historisch Archief Westland",
	"241" : "Gemeentearchief Leidschendam-Voorburg",
	"242" : "Gemeentearchief Wassenaar",
	"244" : "Gemeentearchief Delft",
	"245" : "Gemeentearchief Ede",
	"246" : "Gemeentearchief Gemert-Bakel",
	"247" : "Gemeentearchief Schouwen-Duiveland",
	"248" : "Gemeentearchief Venray",
	"249" : "Gemeentearchief Zoetermeer",
	"250" : "Gemeentearchief Lisse",
	"251" : "Haags Gemeentearchief",
	"252" : "Regionaal Archief Tilburg",
	"253" : "Rijckheyt Centrum voor Regionale Geschiedenis",
	"254" : "West-Brabants Archief",
	"255" : "Erfgoed Leiden en Omstreken"
}

short_archive_names = {
	"211" : "GR_Groningen",
	"213" : "DR_Assen",
	"214" : "OV_Zwolle",
	"215" : "GD_Arnhem",
	"216" : "UT_Utrecht",
	"217" : "NH_Haarlem",
	"218" : "ZH_Nat-Archief",
	"219" : "FR_Friesland",
	"220" : "NB_Den_Bosch",
	"221" : "LB_Maastricht",
	"222" : "FL_Lelystad",
	"223" : "ZH_Rotterdam",
	"224" : "NB_Breda",
	"225" : "ZL_Middelburg",
	"226" : "NB_Eindhoven",
	"229" : "NH_Alkmaar",
	"230" : "Ned-Antillen",
	"231" : "ZH_Oegstgeest",
	"232" : "ZH_Dordrecht",
	"233" : "ZH_Brielle",
	"234" : "ZH_Middelharnis",
	"235" : "ZH_Oudewater",
	"236" : "ZH_Gouda",
	"237" : "ZH_Vlaardingen",
	"238" : "ZH_Alphen_adR",
	"239" : "ZH_Gorinchem",
	"240" : "ZH_Naaldwijk",
	"241" : "ZH_Leidschendam",
	"242" : "ZH_Wassenaar",
	"244" : "ZH_Delft",
	"245" : "GD_Ede",
	"246" : "NB_Gemert",
	"247" : "ZL_Zierikzee",
	"248" : "LB_Venray",
	"249" : "ZH_Zoetermeer",
	"250" : "ZH_Lisse",
	"251" : "ZH_Den_Haag",
	"252" : "NB_Tilburg",
	"253" : "LB_Heerlen",
	"254" : "NB_Bergen_op_Zoom",
	"255" : "ZH_Leiden"
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
#	query_o = "SELECT id_source, registration_maintype, COUNT(*) AS count FROM links_original." + table + " GROUP BY id_source, registration_maintype" + ";"
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
#	query_c = "SELECT id_source, registration_maintype, COUNT(*) AS count FROM links_cleaned." + table + " GROUP BY id_source, registration_maintype" + ";"
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
	print( "host: %s, date: %s" % ( HOST_LINKS, str( now.strftime( "%Y-%m-%d" ) ) ) )
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
	print( "host: %s, date: %s" % ( HOST_LINKS, str( now.strftime( "%Y-%m-%d" ) ) ) )
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
		
		print( "%2d %4s   %-20s %8d       %8d   %7d    %5s" % 
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
	print( "host: %s, date: %s" % ( HOST_LINKS, str( now.strftime( "%Y-%m-%d" ) ) ) )
	print( "============================================================================================" )
	print( "     id        short         links_base   -- c/b loss --  missing  missing   -- b/b loss --" )
	print( " # source  archive name     record count  diff   procent  ego fam  days sb   diff   procent" )
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
		
		print( "%2d %4s   %-20s %8d %8d %5s %7d %7d %7d  %5s" % 
			( n, id_source_str, name, base1, diff_cb, procent_cb_str, ego, days, diff_bb, procent_bb_str ) )

	print( "============================================================================================\n" )



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
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	
	config_path = os.path.join( os.getcwd(), "links_counts.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	HOST_REF   = config.get( "HOST_REF" )
	USER_REF   = config.get( "USER_REF" )
	PASSWD_REF = config.get( "PASSWD_REF" )
	
	db = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )

#	db_check( db )

	print( "host:", HOST_LINKS )
	registration_counts( debug, db )
	sources_person = person_counts( debug, db )
	base_counts( debug, db, sources_person )
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	str_elapsed = format_secs( time() - time0 )
	print( "Counting took %s" % str_elapsed )
	
# [eof]
