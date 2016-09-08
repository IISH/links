#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_reports.py
Version:	0.1
Goal:		collect error log tables into a single table ERROR_STORE
Notice:		See the variable x_codes below. If the ref_report table is updated 
			with new report_type values for 'x' codes, this variable must also 
			be updated. 

07-Sep-2016 Created
07-Sep-2016 Changed
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

debug = True

# db
HOST   = "localhost"
#HOST   = "10.24.64.154"
#HOST   = "10.24.64.158"

USER   = "links"
PASSWD = "mslinks"
DBNAME = ""				# be explicit in all queries

"""
# incomplete
full_archive_names = [ 
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
	{ "id_source" : 231, "name" : "Gemeentearchief Oegstgeest" },
#	{ "id_source" : 231, "name" : "Gemeentearchief Leidschendam-Voorburg" },
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
	{ "id_source" : 242, "name" : "Gemeentearchief Wassenaar" },
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



def none2empty( var ):
	if var is None or var == "None":
		var = ""
	return var



def export_source_type( db, table, id_source, reg_type ):
	if debug: print( "export_source_type() id_source: %s, reg_type: %s" % ( id_source, reg_type ) )
	
	short_name = short_archive_names[ str(id_source) ]
	now = datetime.datetime.now()
	today = now.strftime("%Y-%m-%d")
	filename = "%s_%s_%s_%s.csv" % ( id_source, short_name, reg_type, today )
	print( filename )
	csvfile = open( filename, "w" )
	writer = csv.writer( csvfile )
	
	header = [ "id_log", "id_source", "archive", "location", "reg_type", "date", "sequence", "role", "guid", "content" ]
	writer.writerow( header )
	
	query  = "SELECT * FROM links_logs.`%s` " % table
	query += "WHERE id_source = %d AND reg_type = '%s' AND flag = 2;" % ( id_source, reg_type )
	print( query )
	resp = db.query( query )
	if resp is not None:
		print( resp )
		nrec = len( resp )
		print( "number of records: %d" %nrec )
		for r in range( nrec ):
			rec = resp[ r ]
			if debug: print( "record %d-of-%d" % ( r+1, nrec ) )
			if debug: print( rec )
			
			id_log       = none2empty( rec[ "id_log" ] )
			id_source    = none2empty( rec[ "id_source" ] )
			archive      = none2empty( rec[ "archive" ] )
			location     = none2empty( rec[ "location" ] )
			reg_type     = none2empty( rec[ "reg_type" ] )
			date         = none2empty( rec[ "date" ] )
			sequence     = none2empty( rec[ "sequence" ] )
			role         = none2empty( rec[ "role" ] )
			guid         = none2empty( rec[ "guid" ] )		
			content      = none2empty( rec[ "content" ] )
			
			if debug:
				print( "id_log       = %s" % id_log )
				print( "id_source    = %s" % id_source  )
				print( "archive      = %s" % archive )
				print( "location     = %s" % location )
				print( "reg_type     = %s" % reg_type )
				print( "date         = %s" % date )
				print( "sequence     = %s" % sequence )
				print( "role         = %s" % role )
				print( "guid         = %s" % guid )
				print( "content      = %s" % content )

		line =  [ id_log, id_source, archive, location, reg_type, date, sequence, role, guid, content ]
		writer.writerow( line )

	csvfile.close()



def export( db ):
	if debug: print( "export()" )

	query = "USE links_logs;"
	#print( query )
	resp = db.query( query )
	if resp is None:
		print( "Null response from db" )

	table = "ERROR_STORE"
	
	query = "SELECT COUNT(*) AS count FROM links_logs.`%s`;" % table
	print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "%d records in table %s" % ( count, table ) )

	query = "SELECT COUNT(*) AS count FROM links_logs.`%s` WHERE flag = 2;" % table
	print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "%d records in table %s with flag = 2" % ( count, table ) )

	# which id_source's are involved?
	
	#id_sources = []
	query  = "SELECT id_source, reg_type, COUNT(*) AS count FROM links_logs.`%s` " % table
	query += "WHERE flag = 2 GROUP BY id_source, reg_type;"
	print( query )
	resp = db.query( query )
	if resp is not None:
		ndict = len( resp )
		print( resp )
		for n in range( ndict ):
			count = resp[ n ][ "count" ]
			id_source = resp[ n ][ "id_source" ]
			reg_type  = resp[ n ][ "reg_type" ]
			#id_sources.append( id_source )
			print( "\nnumber of report records for id_source %3s, reg_type %s is %s" % ( id_source, reg_type, count ) )
	
			export_source_type( db, table, id_source, reg_type )



if __name__ == "__main__":
	print( "export_reports.py" )
	
	db = Database( host = HOST, user = USER, passwd = PASSWD, dbname = DBNAME )
	
	export( db )
	
# [eof]
