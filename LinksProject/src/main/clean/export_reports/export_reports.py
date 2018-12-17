#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_reports.py
Version:	0.3
Goal:		Select records from table ERROR_STORE where flag = 2, 
			write a selection of fields to csv files, split by id_source and reg_type. 
TODO:		Read archive_names directly from HOST_REF db

07-Sep-2016 Created
17-Dec-2018 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

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



def none2empty( var ):
	if var is None or var == "None" or var == "null":
		var = ""
	return var



def export_source_type( debug, db, table, id_source, reg_type_in ):
	if debug: print( "export_source_type() id_source: %s, reg_type: %s" % ( id_source, reg_type_in ) )
	
	try:
		short_name = short_archive_names[ str(id_source) ]
	except:
		short_name = "no_source"
	
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
	
#	header = [ "id_log", "id_source", "archive", "location", "reg_type", "error_type", "date", "sequence", "role", "guid", "content" ]
	header = [ "id_log", "id_source", "archive", "location", "reg_type", "date", "sequence", "role", "guid", "error_type", "content" ]
	writer.writerow( header )
	
	query  = "SELECT * FROM links_logs.`%s` " % table
	query += "WHERE id_source = %d AND reg_type = '%s' AND flag = 2;" % ( id_source, reg_type_in )
	if debug: print( query )
	resp = db.query( query )
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
				print( "location   = %s" % location )
				print( "reg_type   = %s" % reg_type_out )
				print( "date       = %s" % date )
				print( "sequence   = %s" % sequence )
				print( "role       = %s" % role )
				print( "guid       = %s" % guid )
				print( "error_type = %s" % error_type )
				print( "content    = %s" % content )
			
			line =  [ id_log, id_source, archive, location, reg_type_out, date, sequence, role, guid, error_type, content ]
			writer.writerow( line )

	csvfile.close()

	# update the ERROR_STORE table
	query  = "UPDATE links_logs.ERROR_STORE SET flag = 3, date_export = '%s', destination = '%s' " % ( today, archive )
	query += "WHERE id_source = %d AND reg_type = '%s' AND flag = 2;" % ( id_source, reg_type_in )
	if debug: print( query )
	resp = db.insert( query )
	if resp is not None and len(resp) != 0:
		print( resp )



def export( debug, db ):
	if debug: print( "export()" )

	query = "USE links_logs;"
	#print( query )
	resp = db.query( query )
	if resp is None:
		print( "Null response from db" )

	table = "ERROR_STORE"
	
	query = "SELECT COUNT(*) AS count FROM links_logs.`%s`;" % table
	if debug: print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "%d records in table %s" % ( count, table ) )

	query = "SELECT COUNT(*) AS count FROM links_logs.`%s` WHERE flag = 2;" % table
	if debug: print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "%d records in table %s with flag = 2" % ( count, table ) )
		if count == 0:
			return

	# which id_source's are involved?
	query  = "SELECT id_source, reg_type, COUNT(*) AS count FROM links_logs.`%s` " % table
	query += "WHERE flag = 2 GROUP BY id_source, reg_type;"
	if debug: print( query )
	resp = db.query( query )
	if resp is not None and len(resp) != 0:
		ndict = len( resp )
		if debug: print( resp )
		for n in range( ndict ):
			rec = resp[ n ]
			count = rec[ "count" ]
			id_source = rec[ "id_source" ]
			reg_type  = rec[ "reg_type" ]
			
			print( "\nnumber of report records for id_source %3s, reg_type %s is %s" % ( id_source, reg_type, count ) )
	
			export_source_type( debug, db, table, id_source, reg_type )



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
	print( "export_reports.py" )
	
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	
	config_path = os.path.join( os.getcwd(), "export_reports.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
#	HOST_REF   = config.get( "HOST_REF" )
#	USER_REF   = config.get( "USER_REF" )
#	PASSWD_REF = config.get( "PASSWD_REF" )
	
	db = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	
	export( debug, db )
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	str_elapsed = format_secs( time() - time0 )
	print( "Exporting Reports took %s" % str_elapsed )
	
# [eof]
