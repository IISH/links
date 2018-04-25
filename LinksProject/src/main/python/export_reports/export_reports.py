#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_reports.py
Version:	0.2
Goal:		Select records from table ERROR_STORE where flag = 2, 
            write a selection of fields to csv files, split by 
            id_source and reg_type. 

07-Sep-2016 Created
25-Apr-2018 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import os
import sys
import datetime
from time import time
import csv
import MySQLdb

debug = False

# db
HOST   = "localhost"

USER   = "links"
PASSWD = "mslinks"
DBNAME = ""				# be explicit in all queries


long_archive_names = { 
	"211" : "Groninger Archieven",
	"212" : "It Tresoar Friesland",
	"213" : "Drents Archief",
	"214" : "Historisch Centrum Overijssel",
	"215" : "Gelders Archief",
	"216" : "Utrechts Archief",
	"217" : "Noord-Hollands Archief",
	"218" : "Nationaal Archief (Zuid-Holland)",
	"219" : "onbekend_archief",
	"220" : "Brabants Historisch Informatie Centrum (BHIC)",
	"221" : "Regionaal Historisch Centrum Limburg",
	"222" : "Nieuw Land Erfgoedcentrum",
	"223" : "Gemeente Archief Rotterdam",
	"224" : "Gemeente Archief Breda",
	"225" : "Zeeuws Archief",
	"226" : "Regionaal Archief Eindhoven",
	"227" : "Archief Eemland",
	"228" : "Gemeente Archief Leeuwarden",
	"229" : "Regionaal Archief Alkmaar",
	"230" : "Nederlandse Antillen",
	"231" : "Gemeente Archief Oegstgeest",
	"232" : "Stadsarchief Dordrecht",
	"233" : "Voorne Putten en Rozenburg",
	"234" : "Goeree Overflakkee",
	"235" : "Streekarchief Rijnstreek",
	"236" : "Streekarchief Midden Holland",
	"237" : "Gemeentearchief Vlaardingen",
	"238" : "Streekarchief Rijnlands Midden",
	"239" : "Gemeentearchief Gorinchem",
	"240" : "Historisch Archief Westland",
	"241" : "Gemeentearchief Leidschendam Voorburg",
	"242" : "Gemeentearchief Wassenaar",
	"243" : "Regionaal Archief Leiden",
	"244" : "Gemeentearchief Delft"
}

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
	
	header = [ "id_log", "id_source", "archive", "location", "reg_type", "error_type", "date_time", "sequence", "role", "guid", "content" ]
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
			error_type   = none2empty( rec[ "report_type" ] )
			date_time    = none2empty( rec[ "date_time" ] )
			sequence     = none2empty( rec[ "sequence" ] )
			role         = none2empty( rec[ "role" ] )
			guid         = none2empty( rec[ "guid" ] )
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
				print( "error_type = %s" % error_type )
				print( "date_time  = %s" % date_time )
				print( "sequence   = %s" % sequence )
				print( "role       = %s" % role )
				print( "guid       = %s" % guid )
				print( "content    = %s" % content )
			
			line =  [ id_log, id_source, archive, location, reg_type_out, error_type, date_time, sequence, role, guid, content ]
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



if __name__ == "__main__":
	print( "export_reports.py" )
	
	db = Database( host = HOST, user = USER, passwd = PASSWD, dbname = DBNAME )
	
	export( debug, db )
	
# [eof]
