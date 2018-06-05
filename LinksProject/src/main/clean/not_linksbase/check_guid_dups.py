# -*- coding: utf-8 -*-

"""
FL-05-Apr-2017
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		check_guid_dups.py
Version:	0.1
Goal:		Check for, and remove, duplicate records by means of checking the guids

05-Apr-2017	Created
05-Apr-2017	Changed

/home/fons/projects/links/clean/not_linksbase/check_guid_dups.py

registration unique identifier: 'uid'
links_a2a.source.recordguid								<-	import_a2a
links_a2a.registration_o_temp.id_persist_registration	<- 	a2a_to_original
links_original.registration_o.id_persist_registration	<- 	a2a_to_original
links_cleaned.registration_c.id_persist_registration	<- 	cleaning
links_prematch.links_base.id_persist_registration		<- 	prematch

-1- The uid is used during cleaning by the functions addToReportRegistration() 
	and addToReportPerson() and written as 'guid' to the log table. 
-2- The cleaning function doRenewData() copies it from registration_o to 
	registration_c. 
-3-	When creating a new links_base table during prematch, the uid is copied 
	from registration_c to links_base. 
-4- The uid values should be unique in the registration tables (registration_o 
	and registration_c). If there are duplicates, it has been forgotten to 
	delete the previous id_source records from registration_o and person_o 
	before loading new sources with a2a_to_original. 
	The uid is not present in person_o or person_c. 
-5- The records of links_base are person-oriented, and therefore duplicates of 
	the uid values do occur, but they always refer to different ego_role values. 
	Duplicates are flagged in registration_c, and do not enter links_base. 
	Multi-dup sets are flagged completely. Of duplicate pairs, the one with the 
	smallest id_registration is kept. 
	Duplicate sources may lead to 'pseudo' multi-dup sets, then too many 
	registrations are flagged. 

Remove of inadvertent uid duplicates. 
-1- Check for duplicates in registration_o and registration_c. 
	If duplicates are found, there are two ways to proceed: 
-2- Either refresh: 
	Delete affected sources from registration_o and person_o, and do a 
	complete 
	-a- ingest,
	-b- clean,
	-c- prematch cycle for those sources. 
-3- Or repair: 
	For affected sources, delete duplicate records from registration_o and 
	registration_c: 
	-a- Either automatic: 
		Set an unique index ('UNI' instead of 'MUL') on the column 
		id_persist_registration in registration_o and registration_c.
	-b- Or manually:
		Keeping one record for each dup set, with either lowest 
		or highest id_registration value. 
	Then, for affected sources, remove those person_o and person_c records 
	that have id_registration values that no longer exist in 
	registration_o / registration_c. 
-4- Duplicate registrations affect the frequency of names, so a new prematch 
	is required: 
	- new frequency tables,
	- new links_base. 
-5- Because of the new frequencies, maybe the LINKS_AUTO records of 
	ref_firstname and ref_familyname must be refreshed. 
	This may be a minor problem; adding new data also has this frequency effect. 
-6- In order to prevent duplicate hassle in the future, deleting obsolete 
	records from links_original must be automatic: 
	include it in the a2a_to_original.py script. 
	 => Scan links_a2a for available id_source's. 
	 	Delete those from registration_o and person_o. 
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
	next, object, oct, open, pow, range, round, super, str, zip )

import datetime
import logging
import MySQLdb
import os
import sys
import yaml

from time import time


log_file = True
dry_run  = True				# True: read only, do not write to db

# settings, read from config file
HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""


class Database:
	def __init__( self, host, user, passwd, dbname ):
	#	print( "host:   %s" % host )
	#	print( "user:   %s" % user )
	#	print( "passwd: %s" % passwd )
	#	print( "dbname: %s" % dbname )
		
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
			logging.error( "%s, %s\n" % ( etype, value ) )
		return affected_count
	
	def update( self, query ):
		return self.insert( query )
	
	def query( self, query ):
		logging.debug( "\n%s" % query )
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



def get_dup_sources( db_links, db_name, table ):
	# return list of id_source's that have guid duplicates
	logging.info( "\nget_dup_sources() %s.%s" % ( db_name, table ) )
	
	query_sel = "SELECT id_source AS source, COUNT(*) as count FROM %s.%s GROUP BY id_source ORDER BY id_source;" % ( db_name, table )
	logging.debug( query_sel )
	resp_sel = db_links.query( query_sel )
#	logging.debug( str( resp_sel ) )
	
	sources = []
	for dict_sel in resp_sel:
		logging.debug( str( dict_sel ) )
		source = dict_sel[ "source" ]
		count  = dict_sel[ "count" ]
		logging.debug( "source: %3d, count: %d" % ( source,  count ) )

		query_dup  = "SELECT COUNT(*) AS dups FROM ("
		query_dup += "  SELECT id_persist_registration, COUNT(*) AS count FROM %s.%s" % ( db_name, table )
		query_dup += "  WHERE id_persist_registration IS NOT NULL AND id_source = %d" % source
		query_dup += "  GROUP BY id_persist_registration HAVING count > 1 "
		query_dup += ") AS t;"
		logging.debug( query_dup )
		resp_dup = db_links.query( query_dup )
		logging.debug( str( resp_dup ) )
		dict_dup = resp_dup[ 0 ]
		
		dict_sel.update( dict_dup )
		logging.info( str( dict_sel ) )
		
		if dict_dup [ "dups" ] > 0:
			sources.append( source )

	return sources



def get_dup_rmtypes( db_links, db_name, table, sources ):
	# return list of registration_maintype's  that have guid duplicates
	logging.info( "\nget_dup_rmtypes() %s.%s" % ( db_name, table ) )
	logging.info( "sources: %s" % str( sources ) )
	
	rmtypes = []
	for source in sources:
		logging.debug( "source: %d" % source )
		
		for rmtype in range(1,4):
			# also want these counts for comparison
			query_sel  = "SELECT id_source AS source, COUNT(*) as count FROM %s.%s " % ( db_name, table )
			query_sel += "WHERE id_source = %d AND registration_maintype = %d;" % ( source, rmtype )
			logging.debug( query_sel )
			resp_sel = db_links.query( query_sel )
			logging.debug( str( resp_sel ) )
			dict_sel = resp_sel[ 0 ]
			count = dict_sel[ "count" ]
			logging.debug( "count: %d" % count )
			
			query_dup  = "SELECT COUNT(*) AS dups FROM ("
			query_dup += "  SELECT registration_maintype AS rmtype, id_persist_registration, COUNT(*) AS count FROM %s.%s" % ( db_name, table )
			query_dup += "  WHERE id_persist_registration IS NOT NULL AND id_source = %d " % source
			query_dup += "  AND registration_maintype = %d " % rmtype
			query_dup += "  GROUP BY id_persist_registration HAVING count > 1 "
			query_dup += ") AS t;"

			logging.debug( query_dup )
			resp_dup = db_links.query( query_dup )
			logging.debug( str( resp_dup ) )
			dict_dup = resp_dup[ 0 ]
			dups = dict_dup [ "dups" ]
			
			dict_resp = {  
				"source" : source, 
				"rmtype" : rmtype, 
				"count"  : count, 
				"dups"   : dups
			}
			logging.info( str( dict_resp ) )
			del dict_resp[ "count" ]
			del dict_resp[ "dups" ]
			
			if dups > 0:
				rmtypes.append( dict_resp )
	
	return rmtypes



def dup_remove( db_links, db_name, table, dup_rmtypes ):
	# get set sets of records that have identical guids; keep one, remove the others. 
	logging.info( "\ndup_remove() %s.%s" % ( db_name, table ) )
	logging.info( "from: %s" % str( dup_rmtypes ) )
	
	for dict_sel in dup_rmtypes:
		source = dict_sel[ "source" ]
		rmtype = dict_sel[ "rmtype" ]
		
		query_dup  = "SELECT GROUP_CONCAT(id_registration), COUNT(*) AS count FROM %s.%s " % ( db_name, table )
		query_dup += "WHERE id_persist_registration IS NOT NULL AND id_source = %d " % source
		query_dup += "AND registration_maintype = %d " % rmtype
		query_dup += "GROUP BY id_persist_registration HAVING count > 1 "
		query_dup += "ORDER BY count DESC, id_registration;";
		
		logging.info( query_dup )
		resp_dup = db_links.query( query_dup )
		logging.info( "# of results: %d" % len( resp_dup ) )
		
		r = 0
		logging.info( "\n" )
		for resp in resp_dup:
			logging.info( resp )
			reg_group = resp[ "GROUP_CONCAT(id_registration)" ]
			print( reg_group )
			# check the data
			regists = reg_group.split( ',' )
			for regist_str in regists:
				regist = int( regist_str )
				query_reg  = "SELECT id_persist_registration FROM %s.%s " % ( db_name, table )
				query_reg += "WHERE id_registration = %s;" % regist
				resp_reg = db_links.query( query_reg )
				dict_reg = resp_reg[ 0 ]
				guid = dict_reg[ "id_persist_registration" ]
				print( "regist: %d, guid: %s" % ( regist, guid ) )
				
				"""
				First re-clean 242 = Wassenaar on notebook, to also get the dups in links_cleaned. 
				Then we can check the flags in registration_c, to select the regs to be deleted: 
				they should be flagged. 
				If all regs of group are flagged, kep the lowest or the highest?
				After deletion, re-clean for the flagging functions.
				"""
				
			r += 1
			if r > 10:
				break



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
	#log_level = logging.DEBUG
	log_level = logging.INFO
	#log_level = logging.WARNING
	#log_level = logging.ERROR
	#log_level = logging.CRITICAL

	if log_file:
		logging_filename = "check_guid_dups.log"
		logging.basicConfig( filename = logging_filename, filemode = 'w', level = log_level )
	else:
		logging.basicConfig( level = log_level )
	
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()
	logging.info( msg )
	if log_file:
		print( msg )
		print( "logging to: %s" % logging_filename )
	
	logging.info( __file__ )

	config_path = os.path.join( os.getcwd(), "check_guid_dups.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	
	
	db_name = "links_original"
	table   = "registration_o"
	
	# get sources that have duplicates
	dup_sources = get_dup_sources( db_links, db_name, table )
	logging.info( "sources with dups: %s" % str( dup_sources ) )
	
	# get rmtypes that have duplicates
	dup_rmtypes = get_dup_rmtypes( db_links, db_name, table, dup_sources )
	logging.info( "sources+rmtypes with dups: %s" % str( dup_rmtypes ) )
	
	"""
	dup_rmtypes = [
		{'source': 242, 'rmtype': 1}, 
		{'source': 242, 'rmtype': 2}, 
		{'source': 242, 'rmtype': 3}
	]
	
	dup_remove( db_links, db_name, table, dup_rmtypes )
	"""
	
	
	db_name = "links_cleaned"
	table   = "registration_c"
	# get sources that have duplicates
	dup_sources = get_dup_sources( db_links, db_name, table )
	logging.info( "sources with dups: %s" % str( dup_sources ) )
	
	# get rmtypes that have duplicates
	dup_rmtypes = get_dup_rmtypes( db_links, db_name, table, dup_sources )
	logging.info( "sources+rmtypes with dups: %s" % str( dup_rmtypes ) )
	


	msg = "Stop: %s" % datetime.datetime.now()
	
	logging.info( msg )
	if log_file: print( msg )
	
	str_elapsed = format_secs( time() - time0 )
	print( "check_guid_dups %s" % str_elapsed )

# [eof]