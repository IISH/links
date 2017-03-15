#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		auto_standardize_names.py
Version:	0.1
Goal:		Automatically standardize names by taking the Levenshtein variant 
			with highest frequency. 
Compatibility:	I used Python-3.6.0; for Py2, csv stuff needs encoding changes

Algorithm:
-1- Loop over table {ref_firstname|ref_familyname}, getting the records with standard_code = 'x'. 
-2- Search for the original in {freq_firstname|freq_familyname}, accepting frequenties of 1 and 2, 
	because only those low frequencies will be automatically normalized. 
-3- Search for the accepted names in {ls_firstname_first|ls_familyname_first}, 
	so obtaining alternative names, require an lvs value of 1. 
-4- Search for the alternative names in {freq_firstname|freq_familyname}, 
	find the name[s] with the highest frequency. Accept the first alternative if 
	its frequency is higher than the frequency of the original. 

class Database:
def db_check( db_links ):
def order_by_freq( resp_lvs, freq_table ):
def find_in_reference( db_ref, ref_table, name_str ):
def normalize_freq_first( db, db_ref, first_or_fam_name ):
def get_lvs_alternatives( lvs_table, name ):
def get_preferred_alt( alts, freq_table ):
def normalize_ref_name( db_links, db_ref, first_or_fam_name ):
def inspect( db, db_ref, freq_table, ref_table ):
def names_with_space( db, db_ref, freq_table, ref_table ):
def compare_first_family( db, db_ref ):
def clear_ref_previous( first_or_fam_name ):
def ref_update( first_or_fam_name, name_ori_esc, name_alt ):
def strip_accents( first_or_fam_name ):
def normalize_ckzsijy( first_or_fam_name ):
def format_secs( seconds ):

TODO Check standard names for remaining non-names like: "--" etc.

13-Apr-2016 Created
15-Mar-2017 Changed
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
	next, object, oct, open, pow, range, round, super, str, zip )

import csv
import datetime
import logging
import MySQLdb
import os
import sys
import unicodedata
import yaml

from collections import Counter
from time import time


log_file = True

# production
ls_table_ext = "_first"			# 
freq_ori_limit = 2				# labtop: firstnames: 1651, familynames: 1386

# test
#ls_table_ext = ""				# 
#freq_ori_limit = 1				# labtop: firstnames: 827, familynames: 829

limit_names = None

# settings, read from config file
HOST_LINKS   = ""
USER_LINKS   = ""
PASSWD_LINKS = ""
DBNAME_LINKS = ""

HOST_REF   = ""
USER_REF   = ""
PASSWD_REF = ""
DBNAME_REF = ""

CREATE_CSV = False

single_quote = "'"
double_quote = '"'


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



def db_check( db_links ):
	# db_name = "links_a2a"
	#tables = [ "a2a", "event", "object", "person", "person_o_temp", "person_profession", 
	#	"registration_o_temp", "relation", "remark", "source", "source_sourceavailablescans_scan" ]
	
	db_name = "links_match"
	tables = [ "match_process", "match_view", "matches", "matrix", "notation" ]

	logging.info( "db_check() %s" % db_name )

	logging.info( "table row counts:" )
	for table in tables:
		query = """SELECT COUNT(*) FROM %s.%s""" % ( db_name, table )
		resp = db_links.query( query )
		if resp is not None:
			count_dict = resp[ 0 ]
			count = count_dict[ "COUNT(*)" ]
			logging.info( "%s %d" % ( table, count ) )
		else:
			logging.info( "Null response from db" )

	# we could show the strings from these GROUPs for cheking, because there should be no variation
	# SELECT eventtype, COUNT(*) FROM links_a2a.event GROUP BY eventtype;
	# SELECT relationtype, COUNT(*) FROM links_a2a.relation GROUP BY relationtype;
	# SELECT relation, COUNT(*) FROM links_a2a.relation GROUP BY relation;
	# SELECT remark_key, COUNT(*) FROM links_a2a.remark GROUP BY remark_key;



def order_by_freq( resp_lvs, freq_table ):
	logging.debug( "order_by_freq()" )
	names_freqs = {}
	
	for d in resp_lvs:
		logging.debug( d )
			
		name_str = d[ "name_str_2" ]
		
		query_freq = "SELECT * FROM links_prematch." + freq_table + " "
		if double_quote in name_str: 
			query_freq += "WHERE name_str = \'" + name_str + "\';"
		else:
			query_freq += "WHERE name_str = \"" + name_str + "\";"
		
		logging.debug( query_freq )
		resp_freq_tup = db_links.query( query_freq )
		
		nitems = len( resp_freq_tup )
		if nitems == 0:
			logging.debug( "order_by_freq() name_str not in %s: %s" % ( freq_table, name_str ) )
		elif nitems == 1:
			resp_freq = resp_freq_tup[ 0 ]
			names_freqs[ name_str ] = resp_freq[ "frequency" ]
		else:
			logging.warning( "order_by_freq() more than 1 hit?" )
			logging.warning( resp_freq_tup )
			logging.warning( "EXIT" )
			sys.exit( 1 )
			
		logging.debug( resp_freq )
		logging.debug( "# of names to process:", nnames )

	sorted_freqs = Counter( names_freqs ).most_common()
	logging.debug( "sorted_freqs:", sorted_freqs )

	return sorted_freqs



def find_in_reference( db_ref, ref_table, name_str ):
	logging.debug( "find_in_reference()"  )
	
	query_ref = "SELECT * FROM links_general." + ref_table + " "
	if double_quote in name_str: 
		query_ref += "WHERE original = \'" + name_str + "\';"
	else:
		query_ref += "WHERE original = \"" + name_str + "\";"
	
	logging.debug( query_ref )

	resp_ref = db_ref.query( query_ref )
	logging.debug( resp_ref )
	
	ref_dict = None
	if len( resp_ref ) == 1:
		ref_dict = resp_ref[ 0 ]
		logging.debug( ref_dict ) 
	else:
		logging.debug( "resp_ref:", resp_ref )
		
	if ref_dict is not None:
		standard_code = ref_dict[ "standard_code" ]
	else:
		standard_code = ""
	
	if ref_dict is not None:
		standard_source = ref_dict[ "standard_source" ]
	else:
		standard_source = ""
	
	return standard_code, standard_source



def normalize_freq_first( debug, db_links, db_ref, first_or_fam_name ):
	logging.debug( "normalize_freq_first() %s" % first_or_fam_name )
	# _freq_first: start with frequency table
	
	if first_or_fam_name == "firstname":
		freq_table = "freq_firstname"
		ls_table   = "ls_firstname" + ls_table_ext
		ref_table  = "ref_firstname"
	elif first_or_fam_name == "familyname":
		freq_table = "freq_familyname"
		ls_table   = "ls_familyname" + ls_table_ext
		ref_table  = "ref_familyname"
	else:
		return
	
	# get names from frequency table that occur only once
	query_freq = "SELECT * FROM links_prematch." + freq_table + " WHERE frequency <= 2 ORDER BY name_str;"
	logging.debug( query_freq )

	resp_freq = db_links.query( query_freq )
	nnames = len( resp_freq )
	logging.debug( resp_freq )
	logging.debug( "# of names in %s that occur at most twice: %d" % ( freq_table, nnames ) )
	
	skip_list = []
	
	n_accept  = 0
	n_discard = 0
	
	# process the names
	for n in range( nnames ):
		logging.debug( "" )
		
		accept = False
		
		dict_freq = resp_freq[ n ]
		name_ori  = dict_freq[ "name_str" ]
		freq_ori  = dict_freq[ "frequency" ]
		
		logging.debug( "dict_freq:", dict_freq )
		logging.debug( "name_ori: %s, freq_ori: %d" % ( name_ori, freq_ori ) )
		
		if single_quote in name_ori and double_quote in name_ori:
			logging.debug( "skipping: %s" % name_ori )
			skip_list.appen( name_ori )
			continue	# hmm, skip this one for the moment
		
		# find in Levenshtein table, for lvs = 1
		query_lvs  = "SELECT * FROM links_prematch." + ls_table + " WHERE value = 1 "
		if double_quote in name_ori: 
			query_lvs += "AND name_str_1 = \'" + name_ori + "\' ORDER BY name_str_2;"
		else: 
			query_lvs += "AND name_str_1 = \"" + name_ori + "\" ORDER BY name_str_2;"
			
		logging.debug( "query_lvs:", query_lvs )
		
		resp_lvs = db_links.query( query_lvs )
		nalts = len( resp_lvs )
		
		logging.debug( "resp_lvs:", resp_lvs )
		logging.debug( "nalts:", nalts )
		
		if nalts == 0:			# nothing
			logging.debug( "# %d: |%s| -> %d Levenshtein=1 alternatives" % ( n, name_ori, nalts ) )
		else:	# choose the alternative with highest frequency with proper standard_code
			if nalts == 1:
				logging.debug( "# %d: |%s| -> %d Levenshtein=1 alternative" % ( n, name_ori, nalts ) )
			else:
				logging.debug( "# %d: |%s| -> %d Levenshtein=1 alternatives" % ( n, name_ori, nalts ) )
				
			name_freqs = order_by_freq( debug, resp_lvs, freq_table )
			logging.debug( "sorted_freqs:", name_freqs )
			
			for name_freq in name_freqs:
				name_std = name_freq[ 0 ]
				standard_code, standard_source = find_in_reference( db_ref, ref_table, name_std )
				freq = name_freq[ 1 ]
				
				# standard_code: 'x' & 'n' not acceptable; 'y' & 'u' acceptable
				# standard_source: "LINKS_AUTO" not acceptable
				if ( standard_code == 'y' or standard_code == 'u' ) and standard_source != "LINKS_AUTO":
					logging.debug( "accept:  standard_code: %s, standard_source: %s |%s| -> |%s| (f=%d)" % 
						( standard_code, standard_source, name_ori, name_std, freq ) )
					
					# update reference query
					query_ref  = "UPDATE links_general." + ref_table + " "
					query_ref += "SET standard = \"" + name_std + "\", "	# standard can have single but not double quote
					query_ref += "standard_code = \"y\", "
					query_ref += "standard_source = \"LINKS_AUTO\" "
					if double_quote in name_ori:
						query_ref += "WHERE original = \'" + name_ori + "\';"
					else:
						query_ref += "WHERE original = \"" + name_ori + "\";"
					
					logging.debug( query_ref )
					
					resp_ref = db_ref.update( query_ref )
					# check response...
					
					accept = True
					break	# done, because accepted, skip further lower freq alternatives
				else:

					logging.debug( "discard: standard_code: %s, standard_source: %s |%s| -> |%s| (f=%d)" % 
						( standard_code, standard_source, name_ori, name_std, freq ) )
				
			logging.debug( "" )
		
		if accept:
			n_accept += 1
		else:
			n_discard += 1
		
		if ( n + 1000 ) % 1000 == 0:
			logging.info( "processed # of names:", n )
		
		if limit_names is not None and n > limit_names:
			msg = "break at %d limit_names:  %d" % limit_names
			logging.info( msg )
			if log_file: print( msg )
			break

	logging.info( "# of names accepted:  %d" % n_accept )
	logging.info( "# of names discarded: %d" % n_discard )

	if len( skip_list ) > 0:
		logging.info( "skipped words:", skip_list )



def get_lvs_alternatives( lvs_table, name ):
	# find in Levenshtein table, for lvs = 1
	logging.debug( "get_lvs_alternatives()" )
	
	#query_lvs  = "SELECT * FROM links_prematch." + lvs_table + " WHERE value = 1 "
	#query_lvs += "AND name_str_1 = \'" + name + "\' ORDER BY name_str_2;"
	
	# this query assumes the lvs_table is an asymmetric (single-sized) lvs table
	query_lvs  = "( SELECT name_int_2 AS name_int, name_str_2 AS name_str FROM links_prematch." + lvs_table + " WHERE value = 1" + " AND name_str_1 = '" + name + "' ) "
	query_lvs += "UNION ALL "
	query_lvs += "( SELECT name_int_1 AS name_int, name_str_1 AS name_str FROM links_prematch." + lvs_table + " WHERE value = 1" + " AND name_str_2 = '" + name + "' ) "
	query_lvs += "ORDER BY name_int;"
#	query_lvs += "ORDER BY name_str;"

	logging.debug( "query_lvs: %s", query_lvs )
#	return 0
		
	resp_lvs = db_links.query( query_lvs )
	nalts = len( resp_lvs )
	
	if nalts > 0:
		logging.debug( "lvs nalts: %d" % nalts )
	
	alts = []
	for n in range( nalts ):
		dict_lvs = resp_lvs[ n ]
		name_str = dict_lvs[ "name_str" ]
		alts.append( name_str )
		logging.debug( "name: %s -> name_str: %s" % ( name, name_str ) )
	
	return alts



def get_preferred_alt( alts, freq_table ):
	# choose alternative with highest frequency
	logging.debug( "get_preferred_alt()" )
	
	name_max = ""
	freq_max = 0
	
	for alt in alts:
		# search frequency of alt in frequency table
		query_freq = "SELECT * FROM links_prematch." + freq_table + " " 
		query_freq += "WHERE name_str = \"" + alt + "\";"
		logging.debug( query_freq )

		resp_freq = db_links.query( query_freq )
		logging.debug( resp_freq )
		
		# check number of hits
		freq_ori = 0
		if len( resp_freq ) == 0:
			logging.debug( "freq: %d, name: |%s| skipped" % ( freq_alt, alt ) )
			continue		# next alt
		elif len( resp_freq ) == 1:
			dict_freq = resp_freq[ 0 ]
			freq_alt  = dict_freq[ "frequency" ]
			logging.debug( "freq: %d, name: %s" % ( freq_alt, alt ) )
			
			if freq_alt > freq_max:
				freq_max = freq_alt
				name_max = alt
			
		else:
			logging.warning( "more than 1 hit from %s?" % freq_table )
			logging.warning( resp_freq )
			logging.warning( "EXIT" )
			sys.exit( 1 )

	return name_max, freq_max



def normalize_ref_name( db_links, db_ref, csv_writer, first_or_fam_name ):
	"""
	normalize_ref_name. 
	-1- get records from (firstname or familyname) reference table where standard_code = 'x'
	-2- get alternative names from levenshtein table with value = 1
	-3- use the alternative with highest value > 1
	"""
	
	msg = "normalize_ref_name() %s" % first_or_fam_name
	logging.info( msg )
	if log_file: print( msg )
	
	if first_or_fam_name == "firstname":
		id_name    = "id_firstname"
		ref_table  = "ref_firstname"
		freq_table = "freq_firstname"
		ls_table   = "ls_firstname" + ls_table_ext
	elif first_or_fam_name == "familyname":
		id_name    = "id_familyname"
		ref_table  = "ref_familyname"
		freq_table = "freq_familyname"
		ls_table   = "ls_familyname" + ls_table_ext
	else:
		return

	# get records from reference table with standard_code 'x'
	query_ref = "SELECT * FROM links_general." + ref_table + " WHERE standard_code = 'x' ORDER BY original;"
	logging.info( query_ref )

	resp_ref = db_ref.query( query_ref )
	nnames = len( resp_ref )
	logging.debug( resp_ref )
	msg = "# of names in %s that have standard_code 'x': %d" % ( ref_table, nnames )
	logging.info( msg )
	if log_file: print( msg )
	
	chunk = None
	nchunks = 1
	if nnames is not None:
		if nnames < 1000:
			nchunks = 1
		elif nnames < 100000:
			nchunks = 10
		else:
			nchunks = 100
		chunk = int( nnames / nchunks )
	print( "nchunks: %d, chunk: %d" % ( nchunks, chunk ) )
	
	n_freq_missing = 0	# not present in frequency table
	n_freq_found   = 0	# found in frequency table
	n_freq_1       = 0	# frequency = 1 in frequency table
	n_freq_2       = 0	# frequency = 2 in frequency table
	n_freq_x       = 0	# frequency > 2 in frequency table
	
	n_lvs_missing = 0	# not present in levenshtein table
	n_lvs_found   = 0	# found in levenshtein table
	
	n_cnt_auto    = 0	# number of ref names set to LINKS_AUTO
		
	# process the 'x' reference records from the reference table
	for n in range( nnames ):
		logging.debug( "" )
		accept = False
		
		#print( ( n + chunk ) % chunk )
		if n > 0 and ( n + chunk ) % chunk == 0:
			msg = "processed # of names: %d" % n
			logging.debug( msg )
			if log_file: print( msg )
		
		dict_ref = resp_ref[ n ]
		id_      = dict_ref[ id_name ]
		name_ori = dict_ref[ "original" ]
		
		logging.debug( "%s: %d, name_ori: %s" % ( id_name, id_, name_ori ) )
		#logging.debug( "escaped:  %s" % str( name_ori ).encode( 'string_escape' ) )
		
		name_ori_esc = name_ori
		name_ori_esc = name_ori_esc.replace( '\\', '\\\\' )
		name_ori_esc = name_ori_esc.replace( "'",  "\\'" )
		name_ori_esc = name_ori_esc.replace( '"',  '\\"' )
		
		if name_ori.find( "'" ) != -1:
			logging.info( "ori: %s, esc: %s" % ( name_ori, name_ori_esc ) )
		
		"""
		if double_quote in name_ori: 
			name_ori_esc = "\'" + name_ori_esc + "\'"
		else:
			name_ori_esc = "\"" + name_ori_esc + "\""
		"""
		
		# search 'original' frequency in frequency table
		query_freq = "SELECT * FROM links_prematch." + freq_table + " " 
		query_freq += "WHERE name_str = \"" + name_ori_esc + "\";"
		logging.debug( query_freq )
		
		#if name_ori == "Catharina\\":
		#	sys.exit( 0 )
		#if name_ori == "'d":
		#	sys.exit( 0 )
		#if name_ori == "schro\"er":
		#	sys.exit( 0 )
		
		resp_freq = db_links.query( query_freq )
		logging.debug( resp_freq )
		
		# check number of hits
		freq_ori = 0
		if len( resp_freq ) == 0:
			logging.debug( "freq: %d, name: |%s| skipped" % ( freq_ori, name_ori ) )
			n_freq_missing += 1
			continue		# next reference record
		elif len( resp_freq ) == 1:
			n_freq_found += 1
			dict_freq = resp_freq[ 0 ]
			freq_ori  = dict_freq[ "frequency" ]
		else:
			logging.warning( "more than 1 hit from %s?" % freq_table )
			logging.warning( resp_freq )
			logging.warning( "EXIT" )
			sys.exit( 1 )
		
		"""
		if freq_ori < 1 or freq_ori > 2:
			logging.debug( "- freq: %d, name: %s" % ( freq_ori, name_ori ) )
			continue
		"""
		
		if freq_ori <= freq_ori_limit:
			logging.debug( "freq: %d, name: %s" % ( freq_ori, name_ori ) )
			if freq_ori == 1:
				n_freq_1 += 1
			elif freq_ori == 2:
				n_freq_2 += 1
			
			alts = get_lvs_alternatives( ls_table, name_ori_esc )
			logging.debug( str( alts ) )
			
			nalts = len( alts )
			if nalts <= 0:
				n_lvs_missing += 1
			else:
				n_lvs_found += 1
			
			name_alt, freq_alt = get_preferred_alt( alts, freq_table )
			
			if freq_alt == 0:
				logging.debug( "no_alt: name_ori: %s; name_alt: %s, freq_alt: %d" % ( name_ori, name_alt, freq_alt ) )
			elif freq_alt > freq_ori:
				n_cnt_auto += 1
				logging.info( "update: name_ori: %s; name_alt: %s, freq_alt: %d" % ( name_ori, name_alt, freq_alt ) )
				if CREATE_CSV:
					csv_writer.writerow( [ name_ori, freq_ori, name_alt, freq_alt, nalts ] )
				# update ref table: 
				ref_update( first_or_fam_name, name_ori_esc, name_alt )
			else:
				logging.debug( "skip:   name_ori: %s; freq_alt: %d, name_alt: %s" % ( name_ori, freq_alt, name_alt ) )
			
		else:
			n_freq_x += 1
			
		if limit_names is not None and n_lvs_found > limit_names:
			msg = "break at %d limit_names:  %d" % limit_names
			logging.debug( msg )
			if log_file: print( msg )
			break
	
	msg = "processed # of names: %d" % nnames 
	logging.info( msg )
	if log_file: print( msg )
	
	logging.info( "n_freq_missing: %d" % n_freq_missing )
	logging.info( "n_freq_found: %d" % n_freq_found )
	logging.info( "n_freq_1: %d" % n_freq_1 )
	logging.info( "n_freq_2: %d" % n_freq_2 )
	logging.info( "n_freq_x: %d" % n_freq_x )
	
	logging.info( "n_lvs_missing: %d" % n_lvs_missing )
	logging.info( "n_lvs_found: %d" % n_lvs_found )

	logging.info( "n_cnt_auto: %d" % n_cnt_auto )

	return n_cnt_auto 



def inspect( db, db_ref, freq_table, ref_table ):
	logging.debug( "inspect()" )
	
	query_freq  = "SELECT name_str, frequency FROM links_prematch." + freq_table + " ORDER BY frequency DESC"
	if limit_names is not None:
		query_freq += " LIMIT str( %d );" % limit_names
	logging.info( query_freq )
	
	resp_freq = db_links.query( query_freq )
	nnames = len( resp_freq )
	logging.debug( resp_freq )
	logging.debug( "# of names to process:", nnames )

	for n in range( nnames ):
		name_dict = resp_freq[ n ]
		logging.debug( name_dict )
		name_str  = name_dict[ "name_str" ]
		frequency = name_dict[ "frequency" ]
		logging.debug( "name: %20s, freq: %5d" % ( name_str, frequency ) )
	
		standard_code, standard_source = find_in_reference( db_ref, ref_table, name_str )
		if standard_code == 'x' or standard_code == '':
			logging.debug( "#: %6d, code: %1s, freq: %6d, name: %s" % ( n, standard_code, frequency, name_str ) )



def names_with_space( db, db_ref, freq_table, ref_table ):
	logging.debug( "names_with_space()" )
	
	query_freq  = "SELECT COUNT(*) AS count FROM links_prematch." + freq_table 
	query_freq += " WHERE INSTR( name_str, ' ' ) > 0"
	logging.info( query_freq )
	
	resp_freq = db_links.query( query_freq )
	nnames = len( resp_freq )
	logging.debug( resp_freq )
	
	count_dict = resp_freq[ 0 ]
	count = count_dict[ "count" ]
	logging.info( "count: %d" % count )
	
	if count == 0:
		return
	
	query_freq  = "SELECT name_str, frequency FROM links_prematch." + freq_table 
	query_freq += " WHERE INSTR( name_str, ' ' ) > 0"
	query_freq += " ORDER BY frequency DESC"
	if limit_names is not None:
		query_freq += " LIMIT str( %d );" % limit_names
	logging.info( query_freq )

	resp_freq = db_links.query( query_freq )
	nnames = len( resp_freq )
	logging.debug( resp_freq )
	logging.info( "# of names to process:", nnames )

	for n in range( nnames ):
		name_dict = resp_freq[ n ]
		logging.debug( name_dict )
		name_str  = name_dict[ "name_str" ]
		frequency = name_dict[ "frequency" ]
		logging.debug( "name: %20s, freq: %5d" % ( name_str, frequency ) )
	
		standard_code, standard_source = find_in_reference( db_ref, ref_table, name_str )
		if standard_code == 'x' or standard_code == '':
			logging.info( "#: %6d, code: %1s, freq: %6d, name: %s" % ( n, standard_code, frequency, name_str ) )



def compare_first_family( db, db_ref ):
	logging.debug( "compare_first_family()" )

	# ref_familyname is about 5 times as large that ref_firstname
	# search ref_firstname.standard in ref_familyname.standard
	# The standard contents may occur multiple times, but we want them only once, -> DISTINCT
	query_fir  = "SELECT DISTINCT(standard) FROM links_general.ref_firstname "
	query_fir += "WHERE standard_code = 'y' "
	query_fir += "ORDER BY standard;"
	
	resp_fir = db_ref.query( query_fir )

	nfir = len( resp_fir )
	logging.info( "# of distinct standard names in ref_firstname", nfir )
	
	# process the names
	cnt = 0
	for n in range( nfir ):
		dict_fir = resp_fir[ n ]
		logging.debug( dict_fir )
		
		standard_fir = dict_fir[ "standard" ]
		logging.debug( standard_fir )
		
		if standard_fir is None or standard_fir == '':
			continue

		query_fam  = "SELECT * FROM links_general.ref_familyname "
		query_fam += "WHERE standard_source = 'GBA' "
		query_fam += "AND standard_code = 'y' "
		query_fam += "AND standard = \"" + standard_fir + "\""
		query_fam += ";"
		
		resp_fam = db_ref.query( query_fam )
		nfam = len( resp_fam )
		
		if nfam > 0:
			cnt += 1
			logging.debug( resp_fam )
			
			dict_fam = resp_fam[ 0 ]
			standard_source = dict_fam[ "standard_source" ]
			
			pct = 100.0 * float(cnt) / float(nfir)
			logging.info( "# %4d: (%6.2f %% done) source: %s; %s" % ( cnt, pct, standard_source, standard_fir ) )
		
		#if ( n + 1000 ) % 1000 == 0:
		#	logging.debug( "processed firstname standards:", n )

			if cnt > 50:
				logging.debug( "break after %d firstname standards" % cnt )
				break



def clear_ref_previous( first_or_fam_name ):
	logging.info( "clear_ref_previous() %s" % first_or_fam_name )
	
	if first_or_fam_name == "firstname":
		ref_table  = "ref_firstname"
	elif first_or_fam_name == "familyname":
		ref_table  = "ref_familyname"
	else:
		return

	query_cnt  = "SELECT COUNT(*) as count FROM links_general.%s " % ref_table
	query_cnt += "WHERE standard_source = 'LINKS_AUTO' "
	query_cnt += ";"
	print( query_cnt )
	
	resp_cnt = db_ref.query( query_cnt )
	dict_cnt = resp_cnt[ 0 ]
	count = dict_cnt[ "count" ]
	if count == 0:
		print( "No LINKS_AUTO records to clear" )
	else:
		print( "Clearing %d %s LINKS_AUTO records" % ( count, first_or_fam_name ) )

		# clear previous automatic normalization: 
		# standard -> NULL, standard_code -> 'x', standard_source -> NULL
		query_clear  = "UPDATE links_general.%s " % ref_table
		query_clear += "SET standard = NULL, standard_code = 'x', standard_source = NULL "
		query_clear += "WHERE standard_source = 'LINKS_AUTO' "
		query_clear += ";"
		
		logging.info( query_clear )
		if log_file: print( query_clear )
		
		affected_count = db_ref.update( query_clear )
		msg = "%d records updated" % affected_count
		logging.info( msg )
		if log_file: print( msg )



def ref_update( first_or_fam_name, name_ori_esc, name_alt ):
	logging.debug( "ref_update() %s" % first_or_fam_name )
	
	if first_or_fam_name == "firstname":
		ref_table  = "ref_firstname"
	elif first_or_fam_name == "familyname":
		ref_table  = "ref_familyname"
	else:
		return

	name_alt_esc = name_alt
	name_alt_esc = name_alt_esc.replace( '\\', '\\\\' )
	name_alt_esc = name_alt_esc.replace( "'",  "\\'" )
	name_alt_esc = name_alt_esc.replace( '"',  '\\"' )

	# update query of reference table
	query_update  = "UPDATE links_general.%s " % ref_table
	query_update += "SET standard = '%s', " % name_alt_esc
	query_update += "standard_code = 'y', "
	query_update += "standard_source = 'LINKS_AUTO' "
	query_update += "WHERE original = '%s' " % name_ori_esc
	query_update += ";"
	logging.debug( query_update )

	affected_count = db_ref.update( query_update )
	if affected_count != 1:
		msg = "%d records updated" % affected_count
		logging.info( msg )
		if log_file: print( msg )



def strip_accents( first_or_fam_name ):
	logging.debug( "strip_accents() %s" % first_or_fam_name )

	if first_or_fam_name == "firstname":
		ref_table  = "ref_firstname"
		id_name_str = "id_firstname"
	elif first_or_fam_name == "familyname":
		ref_table  = "ref_familyname"
		id_name_str = "id_familyname"
	else:
		return
	
	filename = first_or_fam_name + "_standardized.txt"
	cur_dir = os.getcwd()
	pathname = os.path.abspath( os.path.join( cur_dir, filename ) )
	logging.info( "output: %s" % pathname )
	norm_file = open( pathname, 'w' )
	
	query_cnt  = "SELECT COUNT(*) as count FROM links_general.%s;" % ref_table
	logging.info( query_cnt )
	
	resp_cnt = db_ref.query( query_cnt )
	dict_cnt = resp_cnt[ 0 ]
	count = dict_cnt[ "count" ]
	if count == 0:
		logging.info( "No records in %s" % ref_table )
	else:
		logging.info( "Processing %d records from %s;" % ( count, ref_table ) )
		
	limit = None
	query_sel  = "SELECT * FROM links_general.%s " % ref_table
	query_sel += "WHERE standard_code = 'y' "
	query_sel += "ORDER BY standard;"
	
	if limit is not None:
		query_sel += " LIMIT %d" % limit
	query_sel += ";"
	logging.info( query_cnt )

	resp_sel = db_ref.query( query_sel )
	nrec = len( resp_sel )
	naccents = 0

	wierdos = []
	
	accents_ok  = []
	accents_ok += [ "-" ]					# double firstnames, but sometimes linebreak -
	accents_ok += [ "'" ]					# d', l'
	accents_ok += [ "à", "á", "ä", "â" ]
	accents_ok += [ "ç" ]
	accents_ok += [ "è", "é", "ë", "ê" ]
	accents_ok += [ "ì", "í", "ï", "î" ]
	accents_ok += [ "ò", "ó", "ö", "ô" ]
	accents_ok += [ "ù", "ú", "ü", "û" ]
	accents_ok += [      "ý" ]

	if first_or_fam_name == "familyname":
		accents_ok += [ " " ]	# space acceptable in double familyname

	accents_replace = {
		"Æ" : "AE", 
		"æ" : "ae", 
		"ø" : "eu", 
		"Ú" : "é", 
		"Ù" : "ë"
	}

	zap_list = [
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"#", "?", "@", "/", "´", "," 
	#	"÷", "¹", "³", "¾", "¶", "Þ", "¸", "╔"	# autozapped by => ascii
	]
	
	for n in range( nrec ):
		dict_sel = resp_sel[ n ]
		id_name = dict_sel[ id_name_str ]
		standard = dict_sel[ "standard" ]
		if standard is not None:
			
			
			# zap unwanted chars
			old_standard = standard
			for ch in zap_list:
				standard = standard.replace( ch, "" )
			
			if len( old_standard ) != len( standard ):
				logging.info( "zapped: |%s| => |%s|" % ( old_standard, standard ) )
			
			nfkd_form = unicodedata.normalize( "NFKD", standard )
			ascii = str( nfkd_form.encode( "ASCII", "ignore" ), "utf-8" )
			
			if standard != ascii:			# accented word
				naccents += 1
				#logging.info( "standard: |%s| => |%s|" % ( standard, ascii ) )
				norm_file.write( "%s\n" % "standard: |%s| => |%s|" % ( standard, ascii ) )
				
				
				# check lengths, some chars are zapped!
				if len( standard ) != len( ascii ):
					for key in accents_replace:
						nfkd_form = nfkd_form.replace( key, accents_replace[ key] )
					ascii = str( nfkd_form.encode( "ASCII", "ignore" ), "utf-8" )
					logging.info( "replaced: |%s| => |%s|" % ( standard, ascii ) )
				
				# check for weirdo chars
				wierdo = False
				for ch in standard: 
					#if ord( ch ) not in range( 32, 123 ) and ch not in accents_ok:
					if not ch.isalpha() and ch not in accents_ok:
						wierdo = True
						if ch not in wierdos:
							wierdos.append( ch )
				if wierdo:
					logging.info( "wierdo: |%s| => |%s|" % ( standard, ascii ) )
			
			ascii = ascii.strip()	# remove leading and trailing whitespace
			if standard != ascii:
				if ascii == '':
					code = 'n'
				else:
					code = 'y'
				# write ascii as new standard
				query_update  = "UPDATE links_general.%s " % ref_table
				query_update += "SET standard = '%s', " % ascii
				query_update += "standard_code = '%s', " % code
				query_update += "standard_source = 'LINKS_DIACRITIC' "
				query_update += "WHERE %s = %d;" % ( id_name_str, id_name )
				logging.debug( query_update )

				affected_count = db_ref.update( query_update )
		
	logging.info( "\nnames with diacritics: %d (of total %d)" % ( naccents, nrec ) )
	logging.info( "wierdo characters: %s" % str( wierdos ) )
	norm_file.close()



def normalize_ckzsijy( first_or_fam_name ):
	logging.debug( "normalize_ckzsijy() %s" % first_or_fam_name )

	if first_or_fam_name == "firstname":
		ref_table  = "ref_firstname"
	elif first_or_fam_name == "familyname":
		ref_table  = "ref_familyname"
	else:
		return

	query_cnt  = "SELECT COUNT(*) as count FROM links_general.%s;" % ref_table
	logging.info( query_cnt )
	
	resp_cnt = db_ref.query( query_cnt )
	dict_cnt = resp_cnt[ 0 ]
	count = dict_cnt[ "count" ]
	if count == 0:
		logging.info( "No records in %s" % ref_table )
	else:
		logging.info( "Processing %d records from %s" % ( count, ref_table ) )

	limit = None
	query_sel  = "SELECT * FROM links_general.%s ORDER BY standard" % ref_table
	if limit is not None:
		query_sel += " LIMIT %d" % limit
	query_sel += ";"
	logging.info( query_cnt )

	resp_sel = db_ref.query( query_sel )
	nrec = len( resp_sel )
	nksy = 0

	for n in range( nrec ):
		dict_sel = resp_sel[ n ]
		#print( dict_sel )
		standard = dict_sel[ "standard" ]
		if standard is not None:
			standard_ksy = standard
			standard_ksy = standard_ksy.replace( "c", "k" )
			standard_ksy = standard_ksy.replace( "z", "s" )
			standard_ksy = standard_ksy.replace( "ij", "y" )
			
			if standard != standard_ksy:			# ckzsijy stuff
				nksy += 1
				logging.info( "standard: |%s| => |%s|" % ( standard, standard_ksy ) )



	logging.info( "\nnames with new standard_ksy: %d (of total %d)" % ( nksy, nrec ) )



def update_standards( db_ref, first_or_fam_name ):
	"""
	Update standard from original
	"""
	logging.info( "update_standards() %s" % first_or_fam_name )

	if first_or_fam_name == "firstname":
		ref_table  = "ref_firstname"
		id_name_str = "id_firstname"
	elif first_or_fam_name == "familyname":
		ref_table  = "ref_familyname"
		id_name_str = "id_familyname"
	else:
		return

	query_cnt  = "SELECT COUNT(*) as count FROM links_general.%s WHERE standard_code = 'x';" % ref_table
	logging.info( query_cnt )
	
	resp_cnt = db_ref.query( query_cnt )
	dict_cnt = resp_cnt[ 0 ]
	count = dict_cnt[ "count" ]
	if count == 0:
		logging.info( "No records in %s with standard_code = 'x'" % ref_table )
	else:
		logging.info( "Updating standard for %d 'x' records from %s" % ( count, ref_table ) )

	zap_list = [
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"~", "`", "!", "@", "#", "$", "%", "^", "&", "*", "_", "+", "=", 
		"(", ")", "{", "}", "[", "]", 
		"|", "\\", "\:", ";", "\"", "<", ">", ",", ".", "?", "/"
	]

	optional_list = [ "'", "-" ]

	# Because cleaning of the firstnames is currently buggy, we here: 
	# - lowercase original
	# - remove unwanted ascii characters (diacritics are handled separately)
	query_sel  = "SELECT * FROM links_general.%s " % ref_table
	query_sel += "WHERE standard_code = 'x' "
	query_sel += "ORDER BY original;"
	
	limit = None
	if limit is not None:
		query_sel += " LIMIT %d" % limit
	query_sel += ";"
	logging.info( query_cnt )

	resp_sel = db_ref.query( query_sel )
	nrec = len( resp_sel )
	
	zapped  = []
	nzapped = 0
	nspaces = 0
	n_count = 0
	u_count = 0
	y_count = 0
	
	
	for n in range( nrec ):
		dict_sel = resp_sel[ n ]
		id_name = dict_sel[ id_name_str ]
		original = dict_sel[ "original" ]
		if original is None:
			continue
		
		#standard0 = original.lower()
		standard0 = original.lower().casefold()	# For example, the German 
		# lowercase letter 'ß' is equivalent to "ss". Since it is already lowercase, 
		# lower() would do nothing to 'ß'; casefold() converts it to "ss".
		
		standard = ""
		code = ""
		# zap unwanted chars
		standard1 = standard0
		for ch in zap_list:
			if ch in standard1: 
				nzapped += 1
				if ch not in zapped:
					zapped.append( ch )
			standard1 = standard1.replace( ch, "" )
		
		if len( standard1 ) != len( standard0 ):
			code = "u"
			
		if len( standard1 ) == 0:
			standard = ""
			code = "n"
		else:
			# space not admitted in firstname
			if first_or_fam_name == "firstname" and standard1.find( " " ) != -1:
				standard = standard1
				code = "n"
				nspaces += 1
			else:
				if standard1.isalpha():
					standard = standard1
					if code != "u":		# keep 'u'
						code = "y"
				else:	# not all characters are a letters
					nletters = sum( c.isalpha() for c in standard1 )
					if nletters == 0:
						standard = ""
						code = "n"
					else:
						standard = standard1
						code = "y"
						# some chars are optional, but should not be first or last char
						# and they may occur more than once
						while True:
							ch_first = standard[ 0 ]
							if ch_first in optional_list:
								standard = standard[ 1: ]
								code = "u"
							else:
								break
						while True:
							ch_last  = standard[ -1 ]
							if ch_last in optional_list:
								standard = standard[ :-1 ]
								code = "u"
							else:
								break
						
						# and optional chars may not be consecutive
						for ch in optional_list:
							if ch+ch in standard:
								standard = standard.replace( ch+ch, ch )
								code = "u"
		
		# our zapping and replacing may have left us with unwanted leading /trailing whitespace
		s = standard
		standard = standard.strip()
		if s != standard:
			if code == 'y':
				code = 'u'
		
		if code == "n":
			n_count += 1
			logging.info( "garbage: |%s| => |%s| '%s'" % ( original, standard, code ) )
		if code == "u":
			u_count += 1
			logging.info( "changed: |%s| => |%s| '%s'" % ( original, standard, code ) )
		if code == "y":
			y_count += 1

		#standard may contain single quote[s]
		standard_esc = standard.replace( "'",  "\\'" )
		
		query_update  = "UPDATE links_general.%s " % ref_table
		query_update += "SET standard = '%s', standard_code = '%s', standard_source = 'LINKS' " % ( standard_esc, code )
		query_update += "WHERE %s = %d;" % ( id_name_str, id_name )
		logging.debug( query_update )
		
		affected_count = db_ref.update( query_update )
		if affected_count is None:
			logging.warning( query_update )
			logging.info( "nothing updated" )
		elif affected_count != 1:
			logging.warning( query_update )
			logging.info( "%d records updated" % affected_count )
	
	msg = "update_standards %s zapped chars: %s" % ( first_or_fam_name, zapped )
	logging.info( msg ); print( msg )
	msg = "update_standards %s counts: zapped: %d, spaces: %d" % ( first_or_fam_name, nzapped, nspaces )
	logging.info( msg ); print( msg )
	msg = "update_standards %s counts: n: %d, u: %d, y: %d" % ( first_or_fam_name, n_count, u_count, y_count )
	logging.info( msg ); print( msg )
		
		
	"""
	# - LOWER() : buggy cleaning may give originals containing upper case letters
	# - Do not give empty original records a 'y' code
	query_update  = "UPDATE links_general.%s " % ref_table
	query_update += "SET standard = LOWER( original ), standard_code = 'y', standard_source = 'LINKS' "
	query_update += "WHERE standard_code = 'x' AND original <> '';"
	logging.info( query_update )
	
	affected_count = db_ref.update( query_update )
	msg = "%d records updated" % affected_count
	logging.info( msg )

	# Give empty originals a 'n' code
	query_update  = "UPDATE links_general.%s " % ref_table
	query_update += "SET standard = LOWER( original ), standard_code = 'n', standard_source = 'LINKS' "
	query_update += "WHERE standard_code = 'x' AND original = '';"
	logging.info( query_update )
	
	affected_count = db_ref.update( query_update )
	msg = "%d records updated" % affected_count
	logging.info( msg )
	"""



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
		logging_filename = "auto_standardize_names.log"
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
	
	config_path = os.path.join( os.getcwd(), "auto_standardize_names.yaml" )
#	print( "Config file: %s" % config_path )
	config = yaml.safe_load( open( config_path ) )
	
	HOST_LINKS   = config.get( "HOST_LINKS" )
	USER_LINKS   = config.get( "USER_LINKS" )
	PASSWD_LINKS = config.get( "PASSWD_LINKS" )
	
	HOST_REF   = config.get( "HOST_REF" )
	USER_REF   = config.get( "USER_REF" )
	PASSWD_REF = config.get( "PASSWD_REF" )
	
	CREATE_CSV = config.get( "CREATE_CSV" )
	
	msg_links = "links host db: %s \tfor frequency and levenshtein tables" % HOST_LINKS
	msg_ref   = "reference db:  %s \tfor reference tables" % HOST_REF
	logging.info( msg_links )
	logging.info( msg_ref )
	if log_file:
		print( msg_links )
		print( msg_ref )
	
	db_links = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DBNAME_LINKS )
	db_ref   = Database( host = HOST_REF,   user = USER_REF,   passwd = PASSWD_REF,   dbname = DBNAME_REF )
	
#	db_check( db_links )

	print( "Automatic normalization of first- & familynames uses frequency and Levenshtein tables." )
	print( "Those tables must be up-to-date. If they are too old, first run LINKS pre-match." )
	yn = input( "Continue? [n,Y] " )
	if yn.lower() == 'n':
		exit( 0 )
	
	do_first  = input( "Process firstnames? [N,y] " )
	do_family = input( "Process familynames? [N,y] " )
	
	if do_first is None:
		do_first = False
	elif do_first.lower() == 'y':
		do_first = True
	else:
		do_first = False
	
	if do_family is None:
		do_family = False
	elif do_family.lower() == 'y':
		do_family = True
	else:
		do_family = False
	
	if do_first or do_family:
		do_accents = input( "Process diacritics? [N,y] " )
		do_ckzsijy_stuff = input( "Process ckzsijy stuff? [N,y] " )
		do_lowfreq = input( "Process low freqs? [N,y] " )
	
	if do_accents is None:
		do_accents = False
	elif do_accents.lower() == 'y':
		do_accents = True
	else:
		do_accents = False
	
	if do_ckzsijy_stuff is None:
		do_ckzsijy_stuff = False
	elif do_ckzsijy_stuff.lower() == 'y':
		do_ckzsijy_stuff = True
	else:
		do_ckzsijy_stuff = False
		
	if do_lowfreq is None:
		do_lowfreq = False
	elif do_lowfreq.lower() == 'y':
		do_lowfreq = True
	else:
		do_lowfreq = False
		
#	delimiter = str( ',' ).encode( "utf-8" )	# Py2
#	quotechar = str( '"' ).encode( "utf-8" )	# Py2
	delimiter = ','		# Py3
	quotechar = '"'		# Py3
#	quoting   = csv.QUOTE_MINIMAL
	quoting   = csv.QUOTE_NONNUMERIC
	
	if do_first:
		print( "firstname..." )
		print( "update_standards..." )
		update_standards( db_ref, "firstname" )
		
		if do_accents:
			print( "strip_accents..." )
			strip_accents( "firstname" )
		
		if do_ckzsijy_stuff:
			print( "normalize_ckzsijy..." )
			normalize_ckzsijy( "firstname" )
			
		if do_lowfreq:
			print( "do_lowfreq..." )
			if CREATE_CSV:
				csvname_firstname = "firstname"  + ls_table_ext + ".csv"
				msg = "Creating %s ..." % csvname_firstname
				logging.info( msg )
				if log_file: print( msg )
				
				csvfile_firstname = open( csvname_firstname,  'w', newline = '' )
				writer_firstname  = csv.writer( csvfile_firstname,  delimiter = delimiter, quotechar = quotechar, quoting = quoting )
				writer_firstname.writerow( [ "name_ori", "freq_ori", "name_alt", "freq_alt", "num_alts" ] )
			else:
				writer_firstname = None
			
			clear_ref_previous( "firstname" )
			n_cnt_auto = normalize_ref_name( db_links, db_ref, writer_firstname, "firstname" )
			if log_file: print( "%d ref_firstname records set to LINKS_AUTO" % n_cnt_auto )
			
			if CREATE_CSV: csvfile_firstname .close()

	if do_family:
		print( "familyname..." )
		print( "update_standards..." )
		update_standards( db_ref, "familyname" )
		
		if do_accents:
			print( "strip_accents..." )
			strip_accents( "familyname" )
		
		if do_ckzsijy_stuff:
			print( "normalize_ckzsijy..." )
			normalize_ckzsijy( "familyname" )
		
		if do_lowfreq:
			print( "do_lowfreq..." )
			if CREATE_CSV:
				csvname_familyname = "familyname" + ls_table_ext + ".csv"
				msg = "Creating %s ..." % csvname_familyname
				logging.info( msg )
				if log_file: print( msg )
				
				csvfile_familyname = open( csvname_familyname, 'w', newline = '' )
				writer_familyname  = csv.writer( csvfile_familyname, delimiter = delimiter, quotechar = quotechar, quoting = quoting )
				writer_familyname.writerow( [ "name_ori", "freq_ori", "name_alt", "freq_alt", "num_alts" ] )
			else:
				writer_familyname = None
			
			clear_ref_previous( "familyname" )
			n_cnt_auto = normalize_ref_name( db_links, db_ref, writer_familyname, "familyname" )
			if log_file: print( "%d ref_familyname records set to LINKS_AUTO" % n_cnt_auto )
			
			if CREATE_CSV: csvfile_familyname.close()

#	inspect( db_links, db_ref, "freq_firstname",  "ref_firstname" )
#	inspect( db_links, db_ref, "freq_familyname", "ref_familyname" )

#	names_with_space( db_links, db_ref, "freq_firstname",  "ref_firstname" )
#	names_with_space( db_links, db_ref, "freq_familyname", "ref_familyname" )

#	compare_first_family( db_links, db_ref )

	msg = "Stop: %s" % datetime.datetime.now()
	
	logging.info( msg )
	if log_file: print( msg )
	
	str_elapsed = format_secs( time() - time0 )
	print( "Normalization took %s" % str_elapsed )
	
# [eof]
