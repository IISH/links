#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_matches.py
Version:	0.4
Goal:		export matches for CBG
			-1- From table "links_match.MATCHES_CBG_WWW", fetch the records where
				the column Delivering = 'y'. 
				If the number of records is not 1, quit. 
				If the number of records is 1, fetch the value of column "id_match_process". 
			-2- Create CSV file with filename from id_match_process and current date: 
				"idmp=<id_match_process>_MATCHES_EXPORT_<yyyy-mm-dd>.csv"
				The following fields will be written to the CSV file: 			
				header  = [ "Id", "GUID_1", "GUID_2", "Type_Match", "Source_1", "Source_2", "Quality_link_ego_mo", "Quality_link_all", "Worth_link" ]
			-3- Fetch the records from table "links_match.matches" for the given "id_match_process", 
				and write the wanted fields to the CSV file. 
				For the determination of the contents of the 3 fields Quality_link_ego_mo, Quality_link_all, Worth_link, 
				see the function export() below. 
			-4- Update the table "links_match.MATCHES_CBG_WWW":
				query = "UPDATE links_match.MATCHES_CBG_WWW SET Delivering = 'a', Date = '%s' WHERE Delivering = 'y';" % Date

14-Sep-2016 Created
12-Mar-2018 Add column `type_match`
			ALTER TABLE MATCHES_CBG_WWW ADD type_match INT(10) AFTER id_match_process;
13-Mar-2016 Export archive names instead of our id_source numbers
14-Mar-2016 Get archive names now from links_general.ref_source
14-Mar-2018 YAML db config file
03-Apr-2018 Extra CSV field Type_Link
02-May-2018 type_match from links_base ego role
08-May-2018 Strip { and } from GUIDs
17-Sep-2018 Skip records without a GUID
16-Oct-2018 type_match from table match_process
30-Oct-2018 id_match_process 'y' records now from links_match.MATCHES_CBG_WWW
13-Dec-2019 variable links_base and matches table names
18-Dec-2019 Latest Change
09-Sep-2020 id_source_low_limit for export1 method: does not work?
07-Oct-2020 yaml_main & yaml_local
14-Oct-2020 Empty GUID handling bug ('null' was not skipped)
"""

# future-0.17.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, oct, open, pow, range, round, str, super, zip )

import os
import sys
import datetime
import io
from time import time
import csv
import MySQLdb
import yaml

from backports import csv

debug = False
chunk =  10000		# show progress in processing records

id_source_low_limit = 200   # i.e. id_source > id_source_low_limit will be accepted

#limit = 200000		# max number of records, for testing
limit = None		# production

export_source = False	# CBG export
#export_source = True	# our debug


# 13-Mar-2018
# Now the long and short archive names are retrieved, 
# via the id_source number, fom links_general.ref_source table. 
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



def get_base_record( db_links, tbl_base, id_base ):
	if debug: print( "get_base_record()" )
	
	rec = None
	query  = "SELECT * FROM links_prematch.%s WHERE id_base = %s;" % ( tbl_base, id_base )
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None:
		if debug: print( resp )
		rec = resp[ 0 ]
	
	return rec



def get_2base_records( db_links, tbl_base, id_base1, id_base2 ):
	if debug: print( "get_2base_records()" )
	
	rec_id_base1 = None
	rec_id_base2 = None
	
	if id_base1 == id_base2:
		rec_id_base = get_base_record( db_links, tbl_base, id_base1 )
		rec_id_base1 = rec_id_base2 = rec_id_base
	else:
		query = "SELECT * FROM links_prematch.%s WHERE id_base = %s OR id_base = %s;" % ( tbl_base, id_base1, id_base2 )
		if debug: print( query )
		resp = db_links.query( query )
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



def get_type_match( db_ref, rmtype_1, rmtype_2, role_1, role_2 ):
	if debug: print( "get_type_match()" )

	type_match = ""
	
	query  = "SELECT type_match FROM ref_type_match " 
	query += "WHERE s1_main_type = %s AND s2_main_type = %s AND s1_role = %s AND s2_role = %s " % ( rmtype_1, rmtype_2, role_1, role_2 )
	if debug: print( query )
	resp = db_ref.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if nrec == 0:
			print( "No valid type_match record found in ref_type_match for s1_main_type = %s, s2_main_type = %s, s1_role = %s, s2_role = %s " % ( rmtype_1, rmtype_2, role_1, role_2 ) )
		elif nrec == 1:
			rec = resp[ 0 ]
			type_match = rec[ "type_match" ]
	
	return type_match



def export1( db_ref, db_links, tbl_base, tbl_matches, id_match_process, Type_link, export_clariah = False ):
	# export1: GUIDs from tbl_base table via id_base
	# but if tbl_base has been updated after the matching, 
	# this is no longer correct for the updated source/rmtype. 
	print( "export1() for id_match_process %s" % id_match_process )

	low_limit_skip_count = 0

	query = "SELECT COUNT(*) AS count FROM links_match.%s WHERE id_match_process = %s;" % ( tbl_matches, id_match_process )
	print( query )
	resp = db_links.query( query )
	if resp is not None:
		#print( resp )
		count = resp[ 0 ][ "count" ]
		print( "number of matches for id_match_process %s is %d" % ( id_match_process, count ) )
	else:
		print( resp )
	
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
	
	if export_source: 
		header = [ "Id", "GUID_1", "GUID_2", "Type_Match", "Source_1", "Source_2", "Type_link", "Quality_link_A", "Quality_link_B", "Worth_link" ]
	else:	# CBG does not want these 2 columns: source_name_1, source_name_2
		if export_clariah:	# CLARIAH
			header = [ "Id", "Id_Registration_1", "Id_Registration_2", "Type_Match", "Type_link", "Quality_link_A", "Quality_link_B", "Worth_link" ]
		else:				# CBG
			header = [ "Id", "GUID_1", "GUID_2", "Type_Match", "Type_link", "Quality_link_A", "Quality_link_B", "Worth_link" ]
	
	writer.writerow( header )
	
	query = "SELECT * FROM links_match.%s WHERE id_match_process = %s;" % ( tbl_matches, id_match_process )
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if debug: print( "number of records: %d" %nrec )
		
		
		for r in range( nrec ):
			if ( r > 0 and ( r + chunk ) % chunk == 0 ):
				print( "%d-of-%d records processed" % ( r, nrec ) )
			if limit and r >= limit:
				print( "%d-of-%d records processed" % ( r, nrec ) )
				print( "limit reached" )
				break
			
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
			
			rec_linksbase_1, rec_linksbase_2 = get_2base_records( db_links, tbl_base, id_linksbase_1, id_linksbase_2 )
			if rec_linksbase_1 is None or rec_linksbase_2 is None:
				print( "rec_linksbase_1:", rec_linksbase_1, "rec_linksbase_2:", rec_linksbase_2 )
				print( "Could not obtain both tbl_base values; EXIT" )
				sys.exit( 1 )
			
			id_registration_1 = rec_linksbase_1[ "id_registration" ]
			id_registration_2 = rec_linksbase_2[ "id_registration" ]
			
			GUID_1 = none2empty( rec_linksbase_1[ "id_persist_registration" ] )
			GUID_2 = none2empty( rec_linksbase_2[ "id_persist_registration" ] )
			
			if not GUID_1 or not GUID_2:
				continue		# not usable for CBG, skip
			
			# The '{' & '}' come from the CBG XML, but they do not want them in the export data
			if GUID_1:
				GUID_1 = GUID_1.strip( '{}' )
			else:
				GUID_1 = ''
			
			if GUID_2:
				GUID_2 = GUID_2.strip( '{}' )
			else:
				GUID_2 = ''
			
			id_source_1_int = rec_linksbase_1[ "id_source" ]
			id_source_2_int = rec_linksbase_2[ "id_source" ]

			id_source_1 = str( id_source_1_int )
			id_source_2 = str( id_source_2_int )
			
			if id_source_low_limit and ( id_source_1_int <= id_source_low_limit or id_source_2_int <= id_source_low_limit ):
				low_limit_skip_count += 1
				continue
			
			source_name_1, short_name_1 = get_archive_name( db_ref, id_source_1 )
			source_name_2, short_name_2 = get_archive_name( db_ref, id_source_2 )
			
			rmtype_1 = str( rec_linksbase_1[ "registration_maintype" ] )
			rmtype_2 = str( rec_linksbase_2[ "registration_maintype" ] )
			
			role_1 = str( rec_linksbase_1[ "ego_role" ] )
			role_2 = str( rec_linksbase_2[ "ego_role" ] )
				
		#	Type_Match = "mt:%s,ro:%s x mt:%s,ro:%s = %s" % ( rmtype_1, role_1, rmtype_2, role_2, tmatch )
			Type_Match = get_type_match( db_ref, rmtype_1, rmtype_2, role_1, role_2  )	# now from input
			
			Quality_link_A = value_familyname_ego + value_firstname_ego + value_familyname_mo + value_firstname_mo
			Quality_link_B = value_familyname_fa  + value_firstname_fa  + value_familyname_pa + value_firstname_pa
			
			Quality_link_all = Quality_link_A + Quality_link_B
			
			Worth_link = ""
			if Quality_link_A == 0 and Quality_link_B <= 2:
				Worth_link = 3			# "Goed"
			elif Quality_link_A <= 1 and Quality_link_B <= 2:
				Worth_link = 2			# "Hoogstwaarschijnlijk"
			elif Quality_link_A <= 2 and Quality_link_B <= 2:
				Worth_link = 1			# "Waarschijnlijk"
			else:
				Worth_link = 1
				#print( "Quality_link_A: %d, Quality_link_B: %d" % ( Quality_link_A, Quality_link_B ) )
				
			if export_source:
				line = [ Id, GUID_1, GUID_2, Type_Match, source_name_1, source_name_2, Type_link, Quality_link_A, Quality_link_B, Worth_link ]
			else:	# CBG does not want these 2 columns: source_name_1, source_name_2*
				if export_clariah:	# CLARIAH
					line = [ Id, id_registration_1, id_registration_2, Type_Match, Type_link, Quality_link_A, Quality_link_B, Worth_link ]
				else:				# CBG
					line = [ Id, GUID_1, GUID_2, Type_Match, Type_link, Quality_link_A, Quality_link_B, Worth_link ]
			
			writer.writerow( line )
		
		print( "%d-of-%d records processed" % ( nrec, nrec ) )
		
		if id_source_low_limit:
			print( "low_limit_skip_count: %d" % low_limit_skip_count )
		
	csvfile.close()

	update_cbg( db_links, id_match_process )



def export2( db_ref, db_links, tbl_base, tbl_matches, id_match_process, type_match, Type_link ):
	# export2: GUIDs directly from tbl_matches table
	# tbl_base only needed if we also want to export the source names
	print( "export2() for id_match_process %s" % id_match_process )

	filled_guid_recs = 0
	empty_guid_recs  = 0
	
	query = "SELECT COUNT(*) AS count FROM links_match.%s WHERE id_match_process = %s;" % ( tbl_matches, id_match_process )
	print( query )
	resp = db_links.query( query )
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
	
	csvfile = io.open( filepath, "w", encoding  = "utf-8" )
	writer = csv.writer( csvfile )
	
	if export_source: 
		header  = [ "Id", "GUID_1", "GUID_2", "Type_Match", "Source_1", "Source_2", "Type_link", "Quality_link_A", "Quality_link_B", "Worth_link" ]
	else:	# CBG does not want these 2 columns: source_name_1, source_name_2
		header  = [ "Id", "GUID_1", "GUID_2", "Type_Match", "Type_link", "Quality_link_A", "Quality_link_B", "Worth_link" ]
	
	writer.writerow( header )
	
	query = "SELECT * FROM links_match.%s WHERE id_match_process = %s;" % ( tbl_matches, id_match_process )
	if debug: print( query )
	resp = db_links.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if debug: print( "number of records: %d" %nrec )
		
		
		for r in range( nrec ):
			if ( r > 0 and ( r + chunk ) % chunk == 0 ):
				print( "%d-of-%d records processed" % ( r, nrec ) )
			if limit and r >= limit:
				print( "%d-of-%d records processed" % ( r, nrec ) )
				print( "limit reached" )
				break
			
			rec_match = resp[ r ]
			if debug: print( "record %d-of-%d" % ( r+1, nrec ) )
			if debug: print( rec_match )
			
			Id = none2empty( rec_match[ "id_matches" ] )
			
			#Type_Match = none2zero( rec_match[ "type_match" ] )
			
			GUID_1 = none2empty( rec_match[ "id_persist_registration_1" ] )
			GUID_2 = none2empty( rec_match[ "id_persist_registration_2" ] )
			
			if not GUID_1 or not GUID_2:		# Only CBG records have a GUID
			# The '{' & '}' come from the CBG XML, but they do not want them in the export data
				empty_guid_recs += 1
				continue		# not usable for CBG, skip
			
			filled_guid_recs += 1
			GUID_1 = GUID_1.strip( '{}' )
			GUID_2 = GUID_2.strip( '{}' )
			
			value_familyname_ego = none2zero( rec_match[ "value_familyname_ego" ] )
			value_familyname_mo  = none2zero( rec_match[ "value_familyname_mo" ] )
			value_familyname_fa  = none2zero( rec_match[ "value_familyname_fa" ] )
			value_familyname_pa  = none2zero( rec_match[ "value_familyname_pa" ] )
			
			value_firstname_ego = none2zero( rec_match[ "value_firstname_ego" ] )
			value_firstname_mo  = none2zero( rec_match[ "value_firstname_mo" ] )
			value_firstname_fa  = none2zero( rec_match[ "value_firstname_fa" ] )
			value_firstname_pa  = none2zero( rec_match[ "value_firstname_pa" ] )
			
			"""
			id_linksbase_1 = none2empty( rec_match[ "id_linksbase_1" ] )
			id_linksbase_2 = none2empty( rec_match[ "id_linksbase_2" ] )
			
			rec_linksbase_1, rec_linksbase_2 = get_2base_records( db_links, id_linksbase_1, id_linksbase_2 )
			if rec_linksbase_1 is None or rec_linksbase_2 is None:
				print( "rec_linksbase_1:", rec_linksbase_1, "rec_linksbase_2:", rec_linksbase_2 )
				print( "Could not obtain both tbl_base values; EXIT" )
				sys.exit( 1 )
			
			rmtype_1 = str( rec_linksbase_1[ "registration_maintype" ] )
			rmtype_2 = str( rec_linksbase_2[ "registration_maintype" ] )
			
			role_1 = str( rec_linksbase_1[ "ego_role" ] )
			role_2 = str( rec_linksbase_2[ "ego_role" ] )
			
			rmtype_1, role_1 = get_rmtype_role( GUID_1 )
			rmtype_2, role_2 = get_rmtype_role( GUID_2 )
			
		#	Type_Match = "mt:%s,ro:%s x mt:%s,ro:%s = %s" % ( rmtype_1, role_1, rmtype_2, role_2, tmatch )
			Type_Match = get_type_match( db_ref, rmtype_1, rmtype_2, role_1, role_2  )
			"""
			
			Quality_link_A = value_familyname_ego + value_firstname_ego + value_familyname_mo + value_firstname_mo
			Quality_link_B = value_familyname_fa  + value_firstname_fa  + value_familyname_pa + value_firstname_pa
			
			Quality_link_all = Quality_link_A + Quality_link_B
			
			Worth_link = ""
			if Quality_link_A == 0 and Quality_link_B <= 2:
				Worth_link = 3			# "Goed"
			elif Quality_link_A <= 1 and Quality_link_B <= 3:
				Worth_link = 2			# "Hoogstwaarschijnlijk"
			elif Quality_link_A <= 2 and Quality_link_B <= 4:
				Worth_link = 1			# "Waarschijnlijk"
			
			if export_source:
				#id_source_1 = str( rec_linksbase_1[ "id_source" ] )
				#id_source_2 = str( rec_linksbase_2[ "id_source" ] )
				
				# TODO
				#id_source_1 = source_from_guid( GUID_1 )
				#id_source_2 = source_from_guid( GUID_2 )
				
				source_name_1, short_name_1 = get_archive_name( db_ref, id_source_1 )
				source_name_2, short_name_2 = get_archive_name( db_ref, id_source_2 )
			
				line = [ Id, GUID_1, GUID_2, type_match, source_name_1, source_name_2, Type_link, Quality_link_A, Quality_link_B, Worth_link ]
			else:	# CBG does not want these 2 columns: source_name_1, source_name_2*
				line = [ Id, GUID_1, GUID_2, type_match, Type_link, Quality_link_A, Quality_link_B, Worth_link ]
			
			writer.writerow( line )
		
		print( "%d-of-%d records processed" % ( nrec, nrec ) )
		print( "skipped empty_guid_recs: %d" % empty_guid_recs )
		print( "written filled_guid_recs: %d" % filled_guid_recs )
		
	csvfile.close()

	update_cbg( db_links, id_match_process )



def update_cbg( db_links, id_match_process ):
	now = datetime.datetime.now()
	Date = now.strftime( "%Y-%m-%d" )

	query = "UPDATE links_match.MATCHES_CBG_WWW SET Delivering = 'a', Date = '%s' WHERE Delivering = 'y';" % Date
	if debug: print ( query )
	resp = db_links.insert( query )
	if resp is not None and len( resp ) != 0:
		print( "resp:", resp )


"""
def get_id_match_process( db_links ):
	print( "get_id_match_process()" )
	id_match_process = None
	type_link        = None
	
	table = "links_match.MATCHES_CBG_WWW"
	query = "SELECT id_match_process FROM %s WHERE Delivering = 'y'" % table
	if debug: print( query )
	resp = db_links.query( query )
	
	if resp:
		#print( resp )
		nrec = len( resp )
		if nrec == 0:
			print( "No valid id_match_process found with query:" )
			print( query )
		elif nrec == 1:
			rec = resp[ 0 ]
			id_match_process = rec[ "id_match_process" ]
			print( "id_match_process = %s" % id_match_process )
		else:
			print( "Too many id_match_process records were set to 'y', ignoring them all" )
	else:
		print( "No records with Delivering = 'y' in table %s" % table )
	
	if id_match_process:
		table = "links_match.match_process"
		query = "SELECT use_familyname, use_firstname FROM %s WHERE `id` = %s" % ( table, id_match_process )
		resp = db_links.query( query )
		
		if resp:
			#print( resp )
			nrec = len( resp )
			if nrec == 0:
				print( "No valid id_match_process found with query:" )
				print( query )
			elif nrec == 1:
				rec = resp[ 0 ]
				use_familyname = rec[ "use_familyname" ]
				use_firstname  = rec[ "use_firstname" ]
				
				if use_familyname != use_firstname:
					print( "WARNING, unequal: use_familyname = %s, use_firstname = %s" % ( use_familyname, use_firstname ) )
				
				# # EMFP order
				if use_familyname == "1100":
					type_link = '1'
				elif use_familyname == "1110":
					type_link = '2'
				elif use_familyname == "1101":
					type_link = '3'
				elif use_familyname == "1111":
					type_link = '4'
				elif use_familyname == "1001":
					type_link = '5'
				else:
					type_link = ''
				
				print( "id_match_process = %s" % id_match_process )
			else:
				print( "Too many `id` records were set to %s, ignoring them all" % id_match_process )
		else:
			print( "No records with id_match_process = %s in table %s" % ( table, id_match_process ) )
		
	return id_match_process, type_link
"""


def get_idmp_list( db_links ):
	print( "get_idmp_list()" )
	idmp_list = []
	
	table = "links_match.MATCHES_CBG_WWW"
	query = "SELECT id_match_process FROM %s WHERE Delivering = 'y'" % table
	if debug: print( query )
	resp = db_links.query( query )
	for rec in resp:
		idmp = rec[ "id_match_process" ]
		idmp_list.append( idmp )
	
	print( "get_idmp_list: %d idmp 'y' entries" % len( idmp_list ) )
	return idmp_list



def get_type_match_link( id_match_process ):
	print( "get_type_match_link()" )

	table = "links_match.match_process"
	query = "SELECT type_match, use_familyname, use_firstname FROM %s WHERE `id` = %s" % ( table, id_match_process )
	resp = db_links.query( query )
	
	type_match = ''
	type_link  = ''
	if resp:
		rec = resp[ 0 ]
		type_match = rec[ "type_match" ]
		
		use_familyname = rec[ "use_familyname" ]
		use_firstname  = rec[ "use_firstname" ]
		
		if use_familyname != use_firstname:
			print( "WARNING, unequal: use_familyname = %s, use_firstname = %s" % ( use_familyname, use_firstname ) )
		
		# # EMFP order
		if use_familyname == "1100":
			type_link = '1'
		elif use_familyname == "1110":
			type_link = '2'
		elif use_familyname == "1101":
			type_link = '3'
		elif use_familyname == "1111":
			type_link = '4'
		elif use_familyname == "1001":
			type_link = '5'
		else:
			type_link = ''
		
		print( "id_match_process = %s" % id_match_process )
	
	return type_match, type_link



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
#get_yaml_config()



if __name__ == "__main__":
	print( "export_matches.py" )
	
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	
	yaml_filename = "./export_matches.yaml"
	config_local = get_yaml_config( yaml_filename )
	
	YAML_MAIN   = config_local.get( "YAML_MAIN" )
	config_main = get_yaml_config( YAML_MAIN )
	
	HOST_REF   = config_main.get( "HOST_REF" )
	USER_REF   = config_main.get( "USER_REF" )
	PASSWD_REF = config_main.get( "PASSWD_REF" )
	DBNAME_REF = config_main.get( "DBNAME_REF" )
	
	print( "HOST_REF: %s" % HOST_REF )
	print( "USER_REF: %s" % USER_REF )
	print( "PASSWD_REF: %s" % PASSWD_REF )
	print( "DBNAME_REF: %s" % DBNAME_REF )
	
	HOST_LINKS   = config_main.get( "HOST_LINKS" )
	USER_LINKS   = config_main.get( "USER_LINKS" )
	PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
	DBNAME_LINKS = config_main.get( "DBNAME_LINKS" )
	
	print( "HOST_LINKS: %s" % HOST_LINKS )
	print( "USER_LINKS: %s" % USER_LINKS )
	print( "PASSWD_LINKS: %s" % PASSWD_LINKS )
	print( "DBNAME_LINKS: %s" % DBNAME_LINKS )
	
	db_ref   = Database( host = HOST_REF,   user = USER_REF,   passwd = PASSWD_REF,   dbname = DBNAME_REF )
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	
	tbl_base    = config_local.get( "TABLE_BASE" )
	tbl_matches = config_local.get( "TABLE_MATCHES" )
	
	if tbl_base is None or tbl_base == "":
		tbl_base = "links_base"
	if tbl_matches is None or tbl_matches == "":
		tbl_matches = "matches"
	
	print( "Using base table: %s" % tbl_base )
	print( "Using matches table: %s" % tbl_matches )
	
	idmp_list = get_idmp_list( db_links )
	for id_match_process in idmp_list:
		# but type_match empty in match_process table!
		type_match, type_link = get_type_match_link( id_match_process )
		
		print( "id_match_process: %s, type_link: %s" % ( id_match_process, type_link ) )
		#print( "id_match_process: %s" % id_match_process )
		#continue
		
		# Use export1(), because we need additional info from links_base anyway
		# OLD: export1: GUIDs from tbl_base table via id_base
		
		#export_clariah = False	# for separate CHARIAH export
		#export1( db_ref, db_links, tbl_base, tbl_matches, id_match_process, type_link, export_clariah )	# also using tbl_base
		
		# NEW: export2: GUIDs directly from tbl_matches table
		export2( db_ref, db_links, tbl_base, tbl_matches, id_match_process, type_match, type_link )	# not needing tbl_base
		print( '' )
	
	str_elapsed = format_secs( time() - time0 )
	print( "processing took %s" % str_elapsed )
		
# [eof]
