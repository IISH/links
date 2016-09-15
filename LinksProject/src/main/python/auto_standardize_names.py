#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		auto_standardize_names.py
Version:	0.1
Goal:		Automatically standardize names by taking the closest Levenshtein variant. 
			Notice: for standard names from GBA (Gemeentelijke BasisAdministratie persoonsgegevens) 
			we additionally require that the first character must match. 

13-Apr-2016 Created
13-Sep-2016 Changed
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
from collections import Counter

debug = False

limit_names = 20
#limit_names = 100
#limit_names = 250
#limit_names = 1000
#limit_names = 3000

# db
HOST   = "localhost"
#HOST   = "10.24.64.154"
#HOST   = "10.24.64.158"

USER   = "links"
PASSWD = "mslinks"
DBNAME = ""				# be explicit in all queries

"""
HOST_REF   = "10.24.64.30"
USER_REF   = "hsnref"
PASSWD_REF = "refhsn"
DBNAME_REF = ""				# be explicit in all queries
"""
HOST_REF   = "localhost"
USER_REF   = "links"
PASSWD_REF = "mslinks"
DBNAME_REF = ""				# be explicit in all queries

single_quote = "'"
double_quote = '"'

	
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



def order_by_freq( debug, resp_lvs, freq_table ):
	if debug: print( "order_by_freq()" )
	names_freqs = {}
	
	for d in resp_lvs:
		if debug: print( d )
			
		name_str = d[ "name_str_2" ]
		
		query_freq = "SELECT * FROM links_prematch." + freq_table + " "
		if double_quote in name_str: 
			query_freq += "WHERE name_str = \'" + name_str + "\';"
		else:
			query_freq += "WHERE name_str = \"" + name_str + "\";"
		
		if debug:
			print( query_freq )
		resp_freq_tup = db.query( query_freq )
		
		nitems = len( resp_freq_tup )
		if nitems == 0:
			print( "order_by_freq() name_str not in %s: %s" % ( freq_table, name_str ) )
		elif nitems == 1:
			resp_freq = resp_freq_tup[ 0 ]
			names_freqs[ name_str ] = resp_freq[ "frequency" ]
		else:
			print( "order_by_freq() more than 1 hit?" )
			print( resp_freq_tup )
			print( "EXIT" )
			sys.exit( 1 )
			
		if debug: 
			print( resp_freq )
		#	print( "# of names to process:", nnames )

	sorted_freqs = Counter( names_freqs ).most_common()
	#print( "sorted_freqs:", sorted_freqs )

	return sorted_freqs



def find_in_reference( db_ref, ref_table, name_str ):
	if debug: print( "find_in_reference()"  )
	
	query_ref = "SELECT * FROM links_general." + ref_table + " "
	if double_quote in name_str: 
		query_ref += "WHERE original = \'" + name_str + "\';"
	else:
		query_ref += "WHERE original = \"" + name_str + "\";"
	
	if debug: print( query_ref )

	resp_ref = db_ref.query( query_ref )
	#print( resp_ref )
	
	ref_dict = None
	if len( resp_ref ) == 1:
		ref_dict = resp_ref[ 0 ]
		if debug: print( ref_dict ) 
	else:
		if debug: print( "resp_ref:", resp_ref )
		
	if ref_dict is not None:
		standard_code = ref_dict[ "standard_code" ]
	else:
		standard_code = ""
	
	if ref_dict is not None:
		standard_source = ref_dict[ "standard_source" ]
	else:
		standard_source = ""
	
	return standard_code, standard_source



def normalize_freq_first( debug, db, db_ref, first_or_fam_name ):
	print( "normalize_freq_first() %s" % first_or_fam_name )
	# _freq_first: start with frequency table
	
	if first_or_fam_name == "firstname":
		freq_table = "freq_firstname"
		ls_table   = "ls_firstname"
		ref_table  = "ref_firstname"
	elif first_or_fam_name == "familyname":
		freq_table = "freq_familyname"
		ls_table   = "ls_familyname"
		ref_table  = "ref_familyname"
	else:
		return
	
	# get names from frequency table that occur only once
	query_freq = "SELECT * FROM links_prematch." + freq_table + " WHERE frequency <= 2 ORDER BY name_str;"
	print( query_freq )

	resp_freq = db.query( query_freq )
	nnames = len( resp_freq )
	if debug: print( resp_freq )
	print( "# of names in %s that occur at most twice: %d" % ( freq_table, nnames ) )
	
	skip_list = []
	
	n_accept  = 0
	n_discard = 0
	
	# process the names
	for n in range( nnames ):
		if debug: print( "" )
		
		accept = False
		
		dict_freq = resp_freq[ n ]
		name_ori  = dict_freq[ "name_str" ]
		freq_ori  = dict_freq[ "frequency" ]
		
		if debug: print( "dict_freq:", dict_freq )
		if debug: print( "name_ori: %s, freq_ori: %d" % ( name_ori, freq_ori ) )
		
		if single_quote in name_ori and double_quote in name_ori:
			print( "skipping: %s" % name_ori )
			skip_list.appen( name_ori )
			continue	# hmm, skip this one for the moment
		
		# find in Levenshtein table, for lvs = 1
		query_lvs  = "SELECT * FROM links_prematch." + ls_table + " WHERE value = 1 "
		if double_quote in name_ori: 
			query_lvs += "AND name_str_1 = \'" + name_ori + "\' ORDER BY name_str_2;"
		else: 
			query_lvs += "AND name_str_1 = \"" + name_ori + "\" ORDER BY name_str_2;"
			
		if debug: print( "query_lvs:", query_lvs )
		
		resp_lvs = db.query( query_lvs )
		nalts = len( resp_lvs )
		
		if debug: print( "resp_lvs:", resp_lvs )
		if debug: print( "nalts:", nalts )
		
		if nalts == 0:			# nothing
			if debug:
				print( "# %d: |%s| -> %d Levenshtein=1 alternatives" % ( n, name_ori, nalts ) )
		else:	# choose the alternative with highest frequency with proper standard_code
			if debug: 
				if nalts == 1:
					print( "# %d: |%s| -> %d Levenshtein=1 alternative" % ( n, name_ori, nalts ) )
				else:
					print( "# %d: |%s| -> %d Levenshtein=1 alternatives" % ( n, name_ori, nalts ) )
				
			name_freqs = order_by_freq( debug, resp_lvs, freq_table )
			if debug: print( "sorted_freqs:", name_freqs )
			
			for name_freq in name_freqs:
				name_std = name_freq[ 0 ]
				standard_code, standard_source = find_in_reference( db_ref, ref_table, name_std )
				freq = name_freq[ 1 ]
				
				# standard_code: 'x' & 'n' not acceptable; 'y' & 'u' acceptable
				# standard_source: "LINKS_AUTO" not acceptable
				if ( standard_code == 'y' or standard_code == 'u' ) and standard_source != "LINKS_AUTO":
					if debug: 
						print( "accept:  standard_code: %s, standard_source: %s |%s| -> |%s| (f=%d)" % 
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
					
					if debug: print( query_ref )
					
					resp_ref = db_ref.query( query_ref )
					# check response...
					
					accept = True
					break	# done, because accepted, skip further lower freq alternatives
				else:
					if debug: 
						print( "discard: standard_code: %s, standard_source: %s |%s| -> |%s| (f=%d)" % 
							( standard_code, standard_source, name_ori, name_std, freq ) )
				
			if debug: print( "" )
		
		if accept:
			n_accept += 1
		else:
			n_discard += 1
		
		if ( n + 1000 ) % 1000 == 0:
			print( "processed # of names:", n )
		
		#if n > limit_names:
		#	break

	print( "# of names accepted:  %d" % n_accept )
	print( "# of names discarded: %d" % n_discard )

	if len( skip_list ) > 0:
		print( "skipped words:", skip_list )



def normalize_ref_first( debug, db_links, db_ref, first_or_fam_name ):
	"""
	normalize_ref_first. 
	-1- get records from (firstname or familyname) reference table where standard_code = 'x'
	-2- 
	"""
	print( "normalize_ref_first() %s" % first_or_fam_name )
	# _ref_first: start with reference table
	
	if first_or_fam_name == "firstname":
		freq_table = "freq_firstname"
		ls_table   = "ls_firstname"
		ref_table  = "ref_firstname"
	elif first_or_fam_name == "familyname":
		freq_table = "freq_familyname"
		ls_table   = "ls_familyname"
		ref_table  = "ref_familyname"
	else:
		return

	# get records from reference table with standard_code 'x'
	query_ref = "SELECT * FROM links_general." + ref_table + " WHERE standard_code = 'x' ORDER BY original;"
	print( query_ref )

	resp_ref = db_ref.query( query_ref )
	nnames = len( resp_ref )
	if debug: print( resp_ref )
	print( "# of names in %s that have standard_code 'x': %d" % ( ref_table, nnames ) )
	

	n_accept  = 0
	n_discard = 0
	n_freq_0  = 0
	n_freq_1  = 0
	
	# process the 'x' reference records
	for n in range( nnames ):
		if debug: print( "" )
		
		accept = False
		
		dict_ref = resp_ref[ n ]
		name_ori  = dict_ref[ "original" ]
		
		# search 'original' frequency in frequency table
		query_freq = "SELECT * FROM links_prematch." + freq_table + " " 
		if double_quote in name_ori: 
			query_freq += "WHERE name_str = \'" + name_ori + "\';"
		else:
			query_freq += "WHERE name_str = \"" + name_ori + "\";"
		
		if debug: print( query_freq )
		
		resp_freq = db.query( query_freq )
		if debug: print( resp_freq )
		
		# check number of hits
		freq_ori = 0
		if len( resp_freq ) == 0:
			if debug: print( "freq: %d, name: |%s| skipped" % ( freq_ori, name_ori ) )
			n_freq_0 += 1
			continue		# next reference record
		elif len( resp_freq ) == 1:
			n_freq_1 += 1
			dict_freq = resp_freq[ 0 ]
			freq_ori  = dict_freq[ "frequency" ]
		else:
			print( "more than 1 hit from %s?" % freq_table )
			print( resp_freq )
			print( "EXIT" )
			sys.exit( 1 )
		
		"""
		if freq_ori < 1 or freq_ori > 2:
			print( "- freq: %d, name: %s" % ( freq_ori, name_ori ) )
			continue
		"""
		print( "freq: %d, name: %s" % ( freq_ori, name_ori ) )
		
		# ...
		
		if n_freq_1 > limit_names:
			break

	print( "n_freq_0:", n_freq_0 )
	print( "n_freq_1:", n_freq_1 )



def inspect( debug, db, db_ref, freq_table, ref_table ):
	if debug: print( "inspect()" )
	
	query_freq  = "SELECT name_str, frequency FROM links_prematch." + freq_table + " ORDER BY frequency DESC LIMIT "
	query_freq += str( limit_names ) + ";"
	print( query_freq )
	
	resp_freq = db.query( query_freq )
	nnames = len( resp_freq )
	if debug: 
	#	print( resp_freq )
		print( "# of names to process:", nnames )

	for n in range( nnames ):
		name_dict = resp_freq[ n ]
		#print( name_dict )
		name_str  = name_dict[ "name_str" ]
		frequency = name_dict[ "frequency" ]
		#print( "name: %20s, freq: %5d" % ( name_str, frequency ) )
	
		standard_code, standard_source = find_in_reference( db_ref, ref_table, name_str )
		if standard_code == 'x' or standard_code == '':
			print( "#: %6d, code: %1s, freq: %6d, name: %s" % ( n, standard_code, frequency, name_str ) )



def names_with_space( debug, db, db_ref, freq_table, ref_table ):
	if debug: print( "names_with_space()" )
	
	query_freq  = "SELECT COUNT(*) AS count FROM links_prematch." + freq_table 
	query_freq += " WHERE INSTR( name_str, ' ' ) > 0"
	print( query_freq )
	
	resp_freq = db.query( query_freq )
	nnames = len( resp_freq )
	#print( resp_freq )
	
	count_dict = resp_freq[ 0 ]
	count = count_dict[ "count" ]
	print( "count: %d" % count )
	
	if count == 0:
		return
	
	
	query_freq  = "SELECT name_str, frequency FROM links_prematch." + freq_table 
	query_freq += " WHERE INSTR( name_str, ' ' ) > 0"
	query_freq += " ORDER BY frequency DESC LIMIT "
	query_freq += str( limit_names ) + ";"
	print( query_freq )

	resp_freq = db.query( query_freq )
	nnames = len( resp_freq )
	if debug: 
	#	print( resp_freq )
		print( "# of names to process:", nnames )

	for n in range( nnames ):
		name_dict = resp_freq[ n ]
		#print( name_dict )
		name_str  = name_dict[ "name_str" ]
		frequency = name_dict[ "frequency" ]
		#print( "name: %20s, freq: %5d" % ( name_str, frequency ) )
	
		standard_code, standard_source = find_in_reference( db_ref, ref_table, name_str )
		if standard_code == 'x' or standard_code == '':
			print( "#: %6d, code: %1s, freq: %6d, name: %s" % ( n, standard_code, frequency, name_str ) )



def compare_first_family( debug, db, db_ref ):
	if debug: print( "compare_first_family()" )

	# ref_familyname is about 5 times as large that ref_firstname
	# search ref_firstname.standard in ref_familyname.standard
	# The standard contents may occur multiple times, but we want them only once, -> DISTINCT
	query_fir  = "SELECT DISTINCT(standard) FROM links_general.ref_firstname "
	query_fir += "WHERE standard_code = 'y' "
	query_fir += "ORDER BY standard;"
	
	resp_fir = db_ref.query( query_fir )

	nfir = len( resp_fir )
	print( "# of distinct standard names in ref_firstname", nfir )
	
	# process the names
	c = 0
	for n in range( nfir ):
		dict_fir = resp_fir[ n ]
		if debug: print( dict_fir )
		
		standard_fir = dict_fir[ "standard" ]
		if debug: print( standard_fir )
		
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
			c += 1
			#print( resp_fam )
			
			dict_fam = resp_fam[ 0 ]
			standard_source = dict_fam[ "standard_source" ]
			
			p = 100.0 * float(c) / float(nfir)
			print( "# %4d: (%6.2f %% done) source: %s; %s" % ( c, p, standard_source, standard_fir ) )
		
		#if ( n + 1000 ) % 1000 == 0:
		#	print( "processed firstname standards:", n )

			if c > 50:
				break



if __name__ == "__main__":
	print( "links host db: %s \tfor frequency and levenshtein tables" % HOST )
	print( "reference db:  %s \tfor reference tables" % HOST_REF )
	
	db = Database( host = HOST, user = USER, passwd = PASSWD, dbname = DBNAME )

	db_ref = Database( host = HOST_REF, user = USER_REF, passwd = PASSWD_REF, dbname = DBNAME_REF )
	
#	db_check( db )

	print( "Automatic normalization of names use frequency and Levenshtein tables." )
	print( "Those tables must be up-to-date. If they are too old, first run pre-match." )
	yn = input( "Continue? [n,Y] " )
	if yn.lower() == 'n':
		exit( 0 )
	
	do_family = yn = input( "Process familynames? [N,y] " )
	if do_family is not None and do_family.lower() == 'y':
		normalize_ref_first( debug, db, db_ref, "familyname" )
	
	do_first = yn = input( "Process firstnames? [N,y] " )
	if do_first is not None and do_first.lower() == 'y':
		normalize_ref_first( debug, db, db_ref, "firstname" )
	


#	inspect( debug, db, db_ref, "freq_firstname", "ref_firstname" )
#	inspect( debug, db, db_ref, "freq_familyname", "ref_familyname" )

#	names_with_space( debug, db, db_ref, "freq_firstname", "ref_firstname" )
#	names_with_space( debug, db, db_ref, "freq_familyname", "ref_familyname" )

#	compare_first_family( debug, db, db_ref )

# [eof]
