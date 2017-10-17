#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_matches.py
Version:	0.1
Goal:		export matches for CBG

14-Sep-2016 Created
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
import csv
import MySQLdb
import yaml

debug = False
chunk = 10000		# show progress in processing records

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



def get_base_record( id_base ):
	if debug: print( "get_base_record()" )
	
	rec = None
	query  = "SELECT * FROM links_prematch.links_base WHERE id_base = %s;" % id_base
	if debug: print( query )
	resp = db.query( query )
	if resp is not None:
		if debug: print( resp )
		rec = resp[ 0 ]
	
	return rec



def get_2base_records( id_base1, id_base2 ):
	if debug: print( "get_2base_records()" )
	
	rec_id_base1 = None
	rec_id_base2 = None
	
	if id_base1 == id_base2:
		rec_id_base = get_base_record( id_base1 )
		rec_id_base1 = rec_id_base2 = rec_id_base
	else:
		query = "SELECT * FROM links_prematch.links_base WHERE id_base = %s OR id_base = %s;" % ( id_base1, id_base2 )
		if debug: print( query )
		resp = db.query( query )
		if resp is not None:
			nrec = len( resp )
			#print( "nrec: %d" % nrec )
			#print( resp )
			if nrec == 2:
				rec0 = resp[ 0 ]
				rec1 = resp[ 1 ]
				
				if rec0[ "id_base" ] == id_base1: 
					rec_id_base1 = rec0
					rec_id_base2 = rec1
				else:
					rec_id_base1 = rec1
					rec_id_base2 = rec0
	
	return rec_id_base1, rec_id_base2



def none2empty( var ):
	if var is None or var == "None" or var == "null":
		var = ""
	return var



def none2zero( var ):
	ivar = 0
	try:
		ivar = int( var )
	except:
		ivar = 0
		
	return ivar



def export( debug, db, id_match_process ):
	if debug: print( "export() for id_match_process %s" % id_match_process )

	query = "SELECT COUNT(*) AS count FROM links_match.matches WHERE id_match_process = %s;" % id_match_process
	print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "number of matches for id_match_process %s is %d" % ( id_match_process, count ) )

	now = datetime.datetime.now()
	today = now.strftime("%Y-%m-%d")
	filename = "idmp=%s_MATCHES_EXPORT_%s.csv" % ( id_match_process, today )
	print( filename )
	
	filepath =  os.path.join( os.path.dirname(__file__), 'csv', filename )
	if not os.path.exists( os.path.dirname( filepath ) ):
		try:
			os.makedirs( os.path.dirname( filepath ) )
		except: 
			raise
	
	
	csvfile = open( filepath, "w" )
	writer = csv.writer( csvfile )

	header  = [ "Id", "GUI_1", "GUI_2", "Type_1", "Type_2", "Source_1", "Source_2", "Quality_link_ego_mo", "Quality_link_all", "Worth_link" ]
	writer.writerow( header )
	
	query  = "SELECT * FROM links_match.matches WHERE id_match_process = %s;" % id_match_process
	if debug: print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if debug: print( "number of records: %d" %nrec )
		for r in range( nrec ):
			if ( r > 0 and ( r + chunk ) % chunk == 0 ):
				print( "%d-of-%d records processed" % ( r, nrec ) )
			
			rec_match = resp[ r ]
			if debug: print( "record %d-of-%d" % ( r+1, nrec ) )
			if debug: print( rec_match )
			
			Id = none2empty( rec_match[ "id_matches" ] )
			
			id_linksbase_1 = none2empty( rec_match[ "id_linksbase_1" ] )
			id_linksbase_2 = none2empty( rec_match[ "id_linksbase_2" ] )
			
			value_familyname_ego = none2zero( rec_match[ "value_familyname_ego" ] )
			value_familyname_mo  = none2zero( rec_match[ "value_familyname_mo" ] )
			value_familyname_fa  = none2zero( rec_match[ "value_familyname_fa" ] )
			value_familyname_pa  = none2zero( rec_match[ "value_familyname_pa" ] )
			
			value_firstname_ego = none2zero( rec_match[ "value_firstname_ego" ] )
			value_firstname_mo  = none2zero( rec_match[ "value_firstname_mo" ] )
			value_firstname_fa  = none2zero( rec_match[ "value_firstname_fa" ] )
			value_firstname_pa  = none2zero( rec_match[ "value_firstname_pa" ] )
			
			rec_linksbase_1, rec_linksbase_2 = get_2base_records( id_linksbase_1, id_linksbase_2 )
			if rec_linksbase_1 is None or rec_linksbase_2 is None:
				print( "rec_linksbase_1:", rec_linksbase_1, "rec_linksbase_2:", rec_linksbase_2 )
				print( "Could not obtain both links_base values; EXIT" )
				sys.exit( 1 )
			
			GUI_1 = rec_linksbase_1[ "id_persist_registration" ]
			GUI_2 = rec_linksbase_2[ "id_persist_registration" ]
			
			Type_1 = rec_linksbase_1[ "registration_type" ]
			Type_2 = rec_linksbase_2[ "registration_type" ]
			
			Source_1 = rec_linksbase_1[ "id_source" ]
			Source_2 = rec_linksbase_2[ "id_source" ]
			
			Quality_link_ego_mo = value_familyname_ego + value_firstname_ego + value_familyname_mo + value_firstname_mo
			Quality_link_fa_pa  = value_familyname_fa  + value_firstname_fa  + value_familyname_pa + value_firstname_pa
			
			Quality_link_all    = Quality_link_ego_mo + Quality_link_fa_pa
			
			Worth_link = ""
			if Quality_link_ego_mo == 0 and Quality_link_all <= 2:
				Worth_link = "Goed"
			elif Quality_link_ego_mo <= 1 and Quality_link_all <= 3:
				Worth_link = "Hoogstwaarschijnlijk"
			elif Quality_link_ego_mo <= 2 and Quality_link_all <= 4:
				Worth_link = "Waarschijnlijk"
			
			line =  [ Id, GUI_1, GUI_2, Type_1, Type_2, Source_1, Source_2, Quality_link_ego_mo, Quality_link_all, Worth_link ]
			writer.writerow( line )
		
		print( "%d-of-%d records processed" % ( nrec, nrec ) )
		
	csvfile.close()

	update_cbg( db, id_match_process )




def update_cbg( db, id_match_process ):
	now = datetime.datetime.now()
	Date = now.strftime( "%Y-%m-%d" )

	query = "UPDATE links_match.MATCHES_CBG_WWW SET Delivering = 'a', Date = '%s' WHERE Delivering = 'y';" % Date
	if debug: print ( query )
	resp = db.update( query )
	if resp is not None:
		print( "%d records updated", resp )



def get_id_match_process( db ):
	print( "get_id_match_process()" )
	id_match_process = None
	
	query = "SELECT id_match_process FROM links_match.MATCHES_CBG_WWW WHERE Delivering = 'y'"
	if debug: print( query )
	resp = db.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if nrec == 0:
			print( "No valid id_match_process found" )
		elif nrec == 1:
			rec = resp[ 0 ]
			id_match_process = rec[ "id_match_process" ]
			print( "id_match_process = %s" % id_match_process )
		else:
			print( "Too many id_match_process records were set to 'y', ignoring them all" )
			
	return id_match_process



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
	print( "export_matches.py" )
	
	"""
	prompt = "id_match_process: "
	id_match_process = input( "%s" % prompt )
	if id_match_process is None:
		print( "EXIT" )
		sys.exit( 1 )
	"""
	
	time0 = time()		# seconds since the epoch
	
	config_path = os.path.join( os.getcwd(), "export_matches.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	db = Database( host = HOST_LINKS , user = USER_LINKS , passwd = PASSWD_LINKS , dbname = DBNAME_LINKS )
	
	id_match_process = get_id_match_process( db )
	if id_match_process is None:
		sys.exit( 0 )
	
	export( debug, db, id_match_process )
	
	str_elapsed = format_secs( time() - time0 )
	print( "processing took %s" % str_elapsed )
		
# [eof]
