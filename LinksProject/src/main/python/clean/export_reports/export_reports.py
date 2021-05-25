#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_reports.py
Version:	0.5
Goal:		Select records from table links_logs.ERROR_STORE where flag = 2, 
			write a selection of fields to csv files, grouped by reg_type (and 
			optionally id_source). 
			Finally, update the flag value to 3 of the affected records. 
Notice:		Set flag values for exporting to 2: 
			USE links_logs;
			UPDATE links_logs.ERROR_STORE SET flag = 2 WHERE flag = 1;

07-Sep-2016 Created
19-May-2021 Changed
"""

# future-0.17.1 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

import csv
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
"""
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
"""

"""
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
"""


def none2empty( var ):
	if var is None or var == "None" or var == "null":
		var = ""
	return var



def export_source_type( debug, db_ref, db_links, table, id_source, include_id_source, reg_type_in ):
	if debug: print( "export_source_type() id_source: %s, reg_type: %s" % ( id_source, reg_type_in ) )
	
	short_name = "no_source"
	source_name, short_name = get_archive_name( db_ref, id_source )
	
	"""
	try:
		short_name = short_archive_names[ str(id_source) ]
	except:
		short_name = "no_source"
	"""
	
	rtype_fname = none2empty( reg_type_in )
	if rtype_fname == '':
		rtype_fname = "no_type"
				
	now = datetime.datetime.now()
	today = now.strftime( "%Y-%m-%d" )
	filename = "%s_%s_%s_%s.csv" % ( id_source, short_name, rtype_fname, today )
	print( filename )
	
	filepath =  os.path.join( os.path.dirname(__file__), 'csv', filename )
	if not os.path.exists( os.path.dirname( filepath ) ):
		try:
			os.makedirs( os.path.dirname( filepath ) )
		except: 
			raise
	
	
	csvfile = open( filepath, "w" )
	writer = csv.writer( csvfile )
	
	if include_id_source:		# for debugging convenience
		header = [ "id_log", "id_source", "archive", "scan_url", "location", "reg_type", "date", "sequence", "role", "guid", "error_type", "content" ]
	else:						# CBG does not want our id_source
		header = [ "id_log", "archive", "scan_url", "location", "reg_type", "date", "sequence", "role", "guid", "error_type", "content" ]
	writer.writerow( header )
	
	query  = "SELECT * FROM links_logs.`%s` " % table
	query += "WHERE id_source = %d AND reg_type = '%s' AND flag = 2;" % ( id_source, reg_type_in )
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if debug: print( "number of records: %d" %nrec )
		for r in range( nrec ):
			rec = resp[ r ]
			if debug: print( "record %d-of-%d" % ( r+1, nrec ) )
			if debug: print( rec )
			
			id_log       = none2empty( rec[ "id_log" ] )
			id_source    = none2empty( rec[ "id_source" ] )
			archive      = none2empty( rec[ "archive" ] )
			scan_url     = none2empty( rec[ "scan_url" ] )
			location     = none2empty( rec[ "location" ] )
			reg_type_out = none2empty( rec[ "reg_type" ] )
			date         = none2empty( rec[ "date" ] )
			sequence     = none2empty( rec[ "sequence" ] )
			role         = none2empty( rec[ "role" ] )
			guid         = none2empty( rec[ "guid" ] )
			error_type   = none2empty( rec[ "report_type" ] )
			content      = none2empty( rec[ "content" ] )
			
			if archive == '':
				try:
					archive = long_archive_names[ str(id_source) ]
				except:
					archive = "missing_archive_name"
			
			if debug:
				print( "id_log     = %s" % id_log )
				print( "id_source  = %s" % id_source  )
				print( "archive    = %s" % archive )
				print( "scan_url   = %s" % scan_url )
				print( "location   = %s" % location )
				print( "reg_type   = %s" % reg_type_out )
				print( "date       = %s" % date )
				print( "sequence   = %s" % sequence )
				print( "role       = %s" % role )
				print( "guid       = %s" % guid )
				print( "error_type = %s" % error_type )
				print( "content    = %s" % content )
			
			if include_id_source:		# for debugging convenience
				line =  [ id_log, id_source, archive, scan_url, location, reg_type_out, date, sequence, role, guid, error_type, content ]
			else:						# CBG does not want our id_source
				line =  [ id_log, archive, scan_url, location, reg_type_out, date, sequence, role, guid, error_type, content ]
			writer.writerow( line )

	csvfile.close()

	# update the ERROR_STORE table
	query  = "UPDATE links_logs.ERROR_STORE SET flag = 3, date_export = '%s', destination = '%s' " % ( today, archive )
	query += "WHERE id_source = %d AND reg_type = '%s' AND flag = 2;" % ( id_source, reg_type_in )
	if debug: print( query )
	resp = db_links.insert( query )
	if resp is not None:
		print( resp )
# export_source_type()



def export_type( debug, db_ref, db_links, table, include_id_source, reg_type_in ):
	if debug: print( "export_type() reg_type: %s" % reg_type_in )
	
	rtype_fname = none2empty( reg_type_in )
	if rtype_fname == '':
		rtype_fname = "no_type"
				
	now = datetime.datetime.now()
	today = now.strftime( "%Y-%m-%d" )
	filename = "%s_%s.csv" % ( rtype_fname, today )
	print( filename )
	
	filepath =  os.path.join( os.path.dirname(__file__), 'csv', filename )
	if not os.path.exists( os.path.dirname( filepath ) ):
		try:
			os.makedirs( os.path.dirname( filepath ) )
		except: 
			raise
	
	
	csvfile = open( filepath, "w" )
	writer = csv.writer( csvfile )
	
	if include_id_source:		# for debugging convenience
		header = [ "id_log", "id_source", "archive", "scan_url", "location", "reg_type", "date", "sequence", "role", "guid", "error_type", "content" ]
	else:						# CBG does not want our id_source
		header = [ "id_log", "archive", "scan_url", "location", "reg_type", "date", "sequence", "role", "guid", "error_type", "content" ]
	writer.writerow( header )
	
	query  = "SELECT * FROM links_logs.`%s` " % table
	query += "WHERE reg_type = '%s' AND flag = 2;" % reg_type_in
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if debug: print( "number of records: %d" %nrec )
		for r in range( nrec ):
			rec = resp[ r ]
			if debug: print( "record %d-of-%d" % ( r+1, nrec ) )
			if debug: print( rec )
			
			id_log       = none2empty( rec[ "id_log" ] )
			id_source    = none2empty( rec[ "id_source" ] )
			archive      = none2empty( rec[ "archive" ] )
			scan_url     = none2empty( rec[ "scan_url" ] )
			location     = none2empty( rec[ "location" ] )
			reg_type_out = none2empty( rec[ "reg_type" ] )
			date         = none2empty( rec[ "date" ] )
			sequence     = none2empty( rec[ "sequence" ] )
			role         = none2empty( rec[ "role" ] )
			guid         = none2empty( rec[ "guid" ] )
			error_type   = none2empty( rec[ "report_type" ] )
			content      = none2empty( rec[ "content" ] )
			
			if archive == '':
				try:
					archive = long_archive_names[ str(id_source) ]
				except:
					archive = "missing_archive_name"
			
			if debug:
				print( "id_log     = %s" % id_log )
				print( "id_source  = %s" % id_source  )
				print( "archive    = %s" % archive )
				print( "scan_url   = %s" % scan_url )
				print( "location   = %s" % location )
				print( "reg_type   = %s" % reg_type_out )
				print( "date       = %s" % date )
				print( "sequence   = %s" % sequence )
				print( "role       = %s" % role )
				print( "guid       = %s" % guid )
				print( "error_type = %s" % error_type )
				print( "content    = %s" % content )
			
			if include_id_source:		# for debugging convenience
				line =  [ id_log, id_source, archive, scan_url, location, reg_type_out, date, sequence, role, guid, error_type, content ]
			else:						# CBG does not want our id_source
				line =  [ id_log, archive, scan_url, location, reg_type_out, date, sequence, role, guid, error_type, content ]
			writer.writerow( line )

	csvfile.close()

	# update the ERROR_STORE table
	query  = "UPDATE links_logs.ERROR_STORE SET flag = 3, date_export = '%s', destination = '%s' " % ( today, archive )
	query += "WHERE reg_type = '%s' AND flag = 2;" % reg_type_in
	if debug: print( query )
	resp = db_links.insert( query )
	if resp is not None:
		print( resp )
# export_type()



def export( debug, db_ref, db_links, groupby_source, include_id_source ):
	if debug: print( "export()" )

	query = "USE links_logs;"
	#print( query )
	resp = db_links.query( query )
	if resp is None:
		print( "Null response from db" )

	table = "ERROR_STORE"
	
	query = "SELECT COUNT(*) AS count FROM links_logs.`%s`;" % table
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "%d records in table %s" % ( count, table ) )

	query = "SELECT COUNT(*) AS count FROM links_logs.`%s` WHERE flag = 2;" % table
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "%d records in table %s with flag = 2" % ( count, table ) )
		if count == 0:
			return

	# which id_source's and reg_type's are involved?
	query  = "SELECT id_source, reg_type, COUNT(*) AS count FROM links_logs.`%s` " % table
	if groupby_source:
		query += "WHERE flag = 2 GROUP BY id_source, reg_type;"
	else:
		query += "WHERE flag = 2 GROUP BY reg_type;"
	
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None and len(resp) != 0:
		ndict = len( resp )
		if debug: print( resp )
		for n in range( ndict ):
			rec = resp[ n ]
			count = rec[ "count" ]
			reg_type  = rec[ "reg_type" ]
			
			if groupby_source:
				id_source = rec[ "id_source" ]
				print( "\nnumber of report records for id_source %3s, reg_type %s = %s" % ( id_source, reg_type, count ) )
				export_source_type( debug, db_ref, db_links, table, id_source, include_id_source, reg_type )
			else:
				print( "\nnumber of report records for reg_type %s = %s" % ( reg_type, count ) )
				export_type( debug, db_ref, db_links, table, include_id_source, reg_type )
# export()



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
# get_yaml_config()



if __name__ == "__main__":
	print( "export_reports.py" )
	
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	
	yaml_filename = "./export_reports.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	
	#groupby_source = False
	groupby_source = config_local.get( "GROUPBY_SOURCE" )		# multiple csv's ?
	#if GROUPBY_SOURCE in [ "1" "True" ]:
	#	groupby_source = True
	print( "groupby_source: %s" % groupby_source )
	
	#include_id_source = False
	include_id_source = config_local.get( "INCLUDE_ID_SOURCE" )		# id_source in export csv
	#if INCLUDE_ID_SOURCE in [ "1" "True" ]:
	#	include_id_source = True
	print( "include_id_source: %s" % include_id_source )
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )
	
	HOST_LINKS   = config_main.get( "HOST_LINKS" )
	USER_LINKS   = config_main.get( "USER_LINKS" )
	PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
	DBNAME_LINKS = "links_logs"
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	print( "USER_LINKS: %s" % USER_LINKS )
	print( "PASSWD_LINKS: %s" % PASSWD_LINKS )
	
	HOST_REF   = config_main.get( "HOST_REF" )
	USER_REF   = config_main.get( "USER_REF" )
	PASSWD_REF = config_main.get( "PASSWD_REF" )
	DBNAME_REF = "links_general"
	
	print( "HOST_REF: %s" % HOST_REF )
	print( "USER_REF: %s" % USER_REF )
	print( "PASSWD_REF: %s" % PASSWD_REF )
	
	
	main_dir = os.path.dirname( YAML_MAIN )
	sys.path.insert( 0, main_dir )
	from hsn_links_db import Database, format_secs, get_archive_name
	
	print( "Connecting to database at %s" % HOST_REF )
	db_ref = Database( host = HOST_REF, user = USER_REF, passwd = PASSWD_REF, dbname = DBNAME_REF )
	
	print( "Connecting to database at %s" % HOST_LINKS )
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	
	export( debug, db_ref, db_links, groupby_source, include_id_source )
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	str_elapsed = format_secs( time() - time0 )
	print( "Exporting Reports took %s" % str_elapsed )
	
# [eof]
