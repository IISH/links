#!/usr/bin/python
# -*- coding: utf-8-sig -*-

"""
# -*- coding: utf-8 -*-
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		ingest_cbg_csv.py
Version:	0.1
Goal:		Import csv files with CBG data
Notice:		csvkit looks like interesting an intesting helper package
			https://csvkit.readthedocs.io/en/1.0.3/
			$ pip install csvkit
Successfully installed Babel-2.5.3 Unidecode-1.0.22 agate-1.6.1 agate-dbf-0.2.0 
agate-excel-0.2.2 agate-sql-0.5.3 csvkit-1.0.3 dbfread-2.0.7 et-xmlfile-1.0.1 
isodate-0.6.0 jdcal-1.3 leather-0.3.3 openpyxl-2.5.1 parsedatetime-2.4 
python-slugify-1.2.4 pytimeparse-1.1.7 pytz-2018.3 sqlalchemy-1.2.5 xlrd-1.1.0

	$ csvsql -v -i mysql "Export HCO - BS_201802/cbg_geboorte.csv"
	$ csvsql -v -i mysql "Export HCO - BS_201802/cbg_huwelijk.csv" --db-schema cbg_huwelijk-schema
	$ csvsql -v -i mysql "Export HCO - BS_201802/cbg_overlijden.csv"

mysqlimport --delete --ignore-lines=1 --fields-terminated-by=, --fields-enclosed-by=\" --local -u root links_cbg cbg_geboorte.csv
 => last field in filled table starts with a " ??, not specifying line terminator ?

21-Mar-2018 Created
28-Mar-2018 Changed
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
	next, object, oct, open, pow, range, round, super, str, zip )

import os
import sys
import datetime
import logging
import csv
import MySQLdb
import yaml

from time import time


#debug = True
chunk = 10000		# show progress in processing records

# input files from CBG
CSV_DIR = "Export HCO - BS_201802"

CSV_GEB_in = "Export HCO - BS Geboorte_20180122.csv"		# original		
CSV_HUW_in = "Export HCO - BS Huwelijk_20180209.csv"		# original		258185 lines
CSV_OVL_in = "Export HCO - BS Overlijden_20180125.csv"		# original		893462 lines

CSV_GEB_out = "cbg_geboorte.csv"							# 
CSV_HUW_out = "cbg_huwelijk.csv"							# 
CSV_OVL_out = "cbg_overlijden.csv"							# 

# cbg birth column names
cbg_birth_names = {
	"Kind_voornaam" : 0,
	"Kind_patroniem" : 0,
	"Kind_tussenvoegsel" : 0,
	"Kind_achternaam" : 0,
	"Kind_geslacht" : 0,
	"Kind_woonadres" : 0,
	"Kind_datum_geboorte_literal" : 0,
	"Kind_datum_geboorte_YYYY" : 0,
	"Kind_datum_geboorte_MM" : 0,
	"Kind_datum_geboorte_DD" : 0,
	"Vondeling" : 0,
	"Opmerking_kind" : 0,
	"Vader_voornaam" : 0,
	"Vader_patroniem" : 0,
	"Vader_tussenvoegsel" : 0,
	"Vader_achternaam" : 0,
	"Vader_leeftijd_literal" : 0,
	"Vader_leeftijd_Jaar" : 0,
	"Vader_beroep1" : 0,
	"Vader_beroep2" : 0,
	"Moeder_voornaam" : 0,
	"Moeder_patroniem" : 0,
	"Moeder_tussenvoegsel" : 0,
	"Moeder_achternaam" : 0,
	"Moeder_leeftijd_literal" : 0,
	"Moeder_leeftijd_Jaar" : 0,
	"Moeder_beroep1" : 0,
	"Moeder_beroep2" : 0,
	"Kind_datum_doop_literal" : 0,
	"Kind_datum_doop_YYYY" : 0,
	"Kind_datum_doop_MM" : 0,
	"Kind_datum_doop_DD" : 0,
	"Kind_plaats_geboren" : 0,
	"Gemeentenaam" : 0,
	"Akte_datum_literal" : 0,
	"Akte_datum_YYYY" : 0,
	"Akte_datum_MM" : 0,
	"Akte_datum_DD" : 0,
	"Bronsoort" : 0,
	"Plaats_instelling" : 0,
	"Naam_instelling" : 0,
	"Toegangsnummer" : 0,
	"Inventarisnummer" : 0,
	"Aktenummer" : 0,
	"Scan_nummer_1" : 0,
	"Scan_uri_1" : 0,
	"Scan_nummer_2" : 0,
	"Scan_uri_2" : 0,
	"Scan_nummer_3" : 0,
	"Scan_uri_3" : 0,
	"Scan_nummer_4" : 0,
	"Scan_uri_4" : 0,
	"Scan_nummer_5" : 0,
	"Scan_uri_5" : 0,
	"Scan_nummer_6" : 0,
	"Scan_uri_6" : 0,
	"Scan_nummer_7" : 0,
	"Scan_uri_7" : 0,
	"Scan_nummer_8" : 0,
	"Scan_uri_8" : 0,
	"Scan_nummer_9" : 0,
	"Scan_uri_9" : 0,
	"Scan_nummer_10" : 0,
	"Scan_uri_10" : 0,
	"Mutatiedatum" : 0,
	"Scan_URI_Origineel" : 0,
	"RecordGUID" : 0,
	"Opmerking" : 0,
	"AkteSoort" : 0
}

# cbg marriage column names
cbg_marriage_names = {
	"Bruidegom_voornaam" : 0,
	"Bruidegom_patroniem" : 0,
	"Bruidegom_tussenvoegsel" : 0,
	"Bruidegom_achternaam" : 0,
	"Bruidegom_woonplaats" : 0,
	"Bruidegom_leeftijd_literal" : 0,
	"Bruidegom_leeftijd_Jaar" : 0,
	"Bruidegom_datum_geboorte_literal" : 0,
	"Bruidegom_datum_geboorte_YYYY" : 0,
	"Bruidegom_datum_geboorte_MM" : 0,
	"Bruidegom_datum_geboorte_DD" : 0,
	"Bruidegom_geboorteplaats" : 0,
	"Bruidegom_beroep1" : 0,
	"Bruidegom_beroep2" : 0,
	"Bruidegom_vader_voornaam" : 0,
	"Bruidegom_vader_patroniem" : 0,
	"Bruidegom_vader_tussenvoegsel" : 0,
	"Bruidegom_vader_achternaam" : 0,
	"Bruidegom_vader_beroep1" : 0,
	"Bruidegom_vader_beroep2" : 0,
	"Bruidegom_moeder_voornaam" : 0,
	"Bruidegom_moeder_patroniem" : 0,
	"Bruidegom_moeder_tussenvoegsel" : 0,
	"Bruidegom_moeder_achternaam" : 0,
	"Bruidegom_moeder_beroep1" : 0,
	"Bruidegom_moeder_beroep2" : 0,
	"Bruid_voornaam" : 0,
	"Bruid_patroniem" : 0,
	"Bruid_tussenvoegsel" : 0,
	"Bruid_achternaam" : 0,
	"Bruid_woonplaats" : 0,
	"Bruid_leeftijd_literal" : 0,
	"Bruid_leeftijd_Jaar" : 0,
	"Bruid_datum_geboorte_literal" : 0,
	"Bruid_datum_geboorte_YYYY" : 0,
	"Bruid_datum_geboorte_MM" : 0,
	"Bruid_datum_geboorte_DD" : 0,
	"Bruid_geboorteplaats" : 0,
	"Bruid_beroep1" : 0,
	"Bruid_beroep2" : 0,
	"Bruid_vader_voornaam" : 0,
	"Bruid_vader_patroniem" : 0,
	"Bruid_vader_tussenvoegsel" : 0,
	"Bruid_vader_achternaam" : 0,
	"Bruid_vader_beroep1" : 0,
	"Bruid_vader_beroep2" : 0,
	"Bruid_moeder_voornaam" : 0,
	"Bruid_moeder_patroniem" : 0,
	"Bruid_moeder_tussenvoegsel" : 0,
	"Bruid_moeder_achternaam" : 0,
	"Bruid_moeder_beroep1" : 0,
	"Bruid_moeder_beroep2" : 0,
	"Datum_huwelijk_literal" : 0,
	"Datum_huwelijk_YYYY" : 0,
	"Datum_huwelijk_MM" : 0,
	"Datum_huwelijk_DD" : 0,
	"Plaats_huwelijk" : 0,
	"Datum_huwelijksaangifte_literal" : 0,
	"Datum_huwelijksaangifte_YYYY" : 0,
	"Datum_huwelijksaangifte_MM" : 0,
	"Datum_huwelijksaangifte_DD" : 0,
	"Datum_echtscheiding_literal" : 0,
	"Datum_echtscheiding_YYYY" : 0,
	"Datum_echtscheiding_MM" : 0,
	"Datum_echtscheiding_DD" : 0,
	"Gemeentenaam" : 0,
	"Akte_datum_literal" : 0,
	"Akte_datum_YYYY" : 0,
	"Akte_datum_MM" : 0,
	"Akte_datum_DD" : 0,
	"Bronsoort" : 0,
	"Plaats_instelling" : 0,
	"Naam_instelling" : 0,
	"Toegangsnummer" : 0,
	"Inventarisnummer" : 0,
	"Aktenummer" : 0,
	"Scan_nummer_1" : 0,
	"Scan_uri_1" : 0,
	"Scan_nummer_2" : 0,
	"Scan_uri_2" : 0,
	"Scan_nummer_3" : 0,
	"Scan_uri_3" : 0,
	"Scan_nummer_4" : 0,
	"Scan_uri_4" : 0,
	"Scan_nummer_5" : 0,
	"Scan_uri_5" : 0,
	"Scan_nummer_6" : 0,
	"Scan_uri_6" : 0,
	"Scan_nummer_7" : 0,
	"Scan_uri_7" : 0,
	"Scan_nummer_8" : 0,
	"Scan_uri_8" : 0,
	"Scan_nummer_9" : 0,
	"Scan_uri_9" : 0,
	"Scan_nummer_10" : 0,
	"Scan_uri_10" : 0,
	"Mutatiedatum" : 0,
	"Scan_URI_Origineel" : 0,
	"RecordGUID" : 0,
	"Opmerking" : 0,
	"AkteSoort" : 0
}

# cbg death column names
cbg_death_names = {
	"Overledene_voornaam" : 0,
	"Overledene_patroniem" : 0,
	"Overledene_tussenvoegsel" : 0,
	"Overledene_achternaam" : 0,
	"Overledene_geslacht" : 0,
	"Overledene_woonadres" : 0,
	"Overledene_leeftijd_literal" : 0,
	"Overledene_leeftijd_Jaar" : 0,
	"Overledene_datum_geboorte_literal" : 0,
	"Overledene_datum_geboorte_YYYY" : 0,
	"Overledene_datum_geboorte_MM" : 0,
	"Overledene_datum_geboorte_DD" : 0,
	"Overledene_geboorteplaats" : 0,
	"Overledene_beroep1" : 0,
	"Overledene_beroep2" : 0,
	"Overledene_relatie" : 0,
	"Vader_voornaam" : 0,
	"Vader_patroniem" : 0,
	"Vader_tussenvoegsel" : 0,
	"Vader_achternaam" : 0,
	"Vader_beroep1" : 0,
	"Vader_beroep2" : 0,
	"Moeder_voornaam" : 0,
	"Moeder_patroniem" : 0,
	"Moeder_tussenvoegsel" : 0,
	"Moeder_achternaam" : 0,
	"Moeder_beroep1" : 0,
	"Moeder_beroep2" : 0,
	"Relatie1_voornaam" : 0,
	"Relatie1_patroniem" : 0,
	"Relatie1_tussenvoegsel" : 0,
	"Relatie1_achternaam" : 0,
	"Relatie1_geslacht" : 0,
	"Relatie1_beroep1" : 0,
	"Relatie1_beroep2" : 0,
	"Relatie1_type" : 0,
	"Relatie2_voornaam" : 0,
	"Relatie2_patroniem" : 0,
	"Relatie2_tussenvoegsel" : 0,
	"Relatie2_achternaam" : 0,
	"Relatie2_geslacht" : 0,
	"Relatie2_beroep1" : 0,
	"Relatie2_beroep2" : 0,
	"Relatie2_type" : 0,
	"Relatie3_voornaam" : 0,
	"Relatie3_patroniem" : 0,
	"Relatie3_tussenvoegsel" : 0,
	"Relatie3_achternaam" : 0,
	"Relatie3_geslacht" : 0,
	"Relatie3_beroep1" : 0,
	"Relatie3_beroep2" : 0,
	"Relatie3_type" : 0,
	"Relatie4_voornaam" : 0,
	"Relatie4_patroniem" : 0,
	"Relatie4_tussenvoegsel" : 0,
	"Relatie4_achternaam" : 0,
	"Relatie4_geslacht" : 0,
	"Relatie4_beroep1" : 0,
	"Relatie4_beroep2" : 0,
	"Relatie4_type" : 0,
	"Relatie5_voornaam" : 0,
	"Relatie5_patroniem" : 0,
	"Relatie5_tussenvoegsel" : 0,
	"Relatie5_achternaam" : 0,
	"Relatie5_geslacht" : 0,
	"Relatie5_beroep1" : 0,
	"Relatie5_beroep2" : 0,
	"Relatie5_type" : 0,
	"Overledene_datum_overlijden_literal" : 0,
	"Overledene_datum_overlijden_YYYY" : 0,
	"Overledene_datum_overlijden_MM" : 0,
	"Overledene_datum_overlijden_DD" : 0,
	"Overledene_plaats_overlijden" : 0,
	"Gemeentenaam" : 0,
	"Akte_datum_literal" : 0,
	"Akte_datum_YYYY" : 0,
	"Akte_datum_MM" : 0,
	"Akte_datum_DD" : 0,
	"Bronsoort" : 0,
	"Plaats_instelling" : 0,
	"Naam_instelling" : 0,
	"Toegangsnummer" : 0,
	"Inventarisnummer" : 0,
	"Aktenummer" : 0,
	"Scan_nummer_1" : 0,
	"Scan_uri_1" : 0,
	"Scan_nummer_2" : 0,
	"Scan_uri_2" : 0,
	"Scan_nummer_3" : 0,
	"Scan_uri_3" : 0,
	"Scan_nummer_4" : 0,
	"Scan_uri_4" : 0,
	"Scan_nummer_5" : 0,
	"Scan_uri_5" : 0,
	"Scan_nummer_6" : 0,
	"Scan_uri_6" : 0,
	"Scan_nummer_7" : 0,
	"Scan_uri_7" : 0,
	"Scan_nummer_8" : 0,
	"Scan_uri_8" : 0,
	"Scan_nummer_9" : 0,
	"Scan_uri_9" : 0,
	"Scan_nummer_10" : 0,
	"Scan_uri_10" : 0,
	"Mutatiedatum" : 0,
	"Scan_URI_Origineel" : 0,
	"RecordGUID" : 0,
	"Opmerking" : 0,
	"AkteSoort" : 0
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
		as exception. See the MySQL docs for mysqnl_info(), and the Python warning module. (Non-standard)
		"""
		return self.connection.info()

	def __del__( self ):
		self.connection.close()



def read_csv( csv_dir, csv_filename_in, csv_filename_out, cbg_names ):
	csv_pathname_in  = os.path.abspath( os.path.join( csv_dir, csv_filename_in ) )
	csv_pathname_out = os.path.abspath( os.path.join( csv_dir, csv_filename_out ) )
	
	msg = "input:  %s" % csv_pathname_in
	print( msg ); logging.info( msg )
	msg = "output: %s" % csv_pathname_out
	print( msg ); logging.info( msg )
	
	# '\ufeff' = tBOM = he Byte Order Mark of feff is an indicator of utf-16 encoding.
	csv_file_in  = open( csv_pathname_in,  'r', encoding = 'utf-8-sig' )
	csv_file_out = open( csv_pathname_out, 'w', encoding = 'utf-8' )
		
	ncbg_names = len( cbg_names )
	
	for key, val in cbg_names.items():
		logging.debug( "key: %s, val: %d" % ( key, val ) )
	
	use_limit = False
	max_lines = 100
	nline = 0
	header_fields = []
	
	nfields_header = 0
	ndquotes_header = 0
	ncomma_header = 0
	nbslash = 0
	
	fields = []
	nl_skip = 0
	nl_out = 0
	
#	for fields in csv.reader( csv_file, delimiter = ',', quotechar = '"' ):
		
	for l, line in enumerate( csv_file_in ):
		line = line.strip()		# remove trailing whitespace
		
		if l == 0:
			header_fields = line.split( ',' )
			nfields_header = len( header_fields )
			
			msg = "# of header fields: %d" % nfields_header
			print( msg ); logging.info( msg )
			
			ndquotes_header = line.count( '"' )
			ncomma_header  = line.count( ',' )
			
			msg = "# of dquotes: %d" % ndquotes_header
			print( msg ); logging.info( msg )
			msg = "# of commas:  %d" % ncomma_header 
			print( msg ); logging.info( msg )
			
			nl_out += 1
			csv_file_out.write( "%s\n" % line )
		else:
			ndquotes = line.count( '"' )
			ncomma   = line.count( ',' )
			nbslash  = line.count( '\\' )
			
		#	if ndquotes != ndquotes_header:
		#	if ndquotes != ndquotes_header or ncomma != ncomma_header:
			if ndquotes != ndquotes_header or nbslash > 0:
				nl_skip += 1
				fields = line.split( ',' )
				remark = fields[ -2 ]
				
			#	msg = "line: %6d, dquote count: %d, comma count: %d, remark: %s" % ( l, ndquotes, ncomma, remark )
				if nbslash > 0:
					msg = "line: %6d, dquote count: %d, bslash count: %d, remark: %s" % ( l, ndquotes, nbslash, remark )
				else:
					msg = "line: %6d, dquote count: %d, remark: %s" % ( l, ndquotes, remark )
				
				print( msg ); logging.info( msg )
			#	print( "%s\n" % line )
			else:
				nl_out += 1
				csv_file_out.write( "%s\n" % line )
				
			continue
		
		
		if ( nline > 0 and ( nline + chunk ) % chunk == 0 ):
			print( "%d lines processed" % ( nline ) )
		
		nline += 1
		
	#	line = line.strip( '\n' )			# remove trailing \n
		logging.debug( "fields %d: %s" % ( nline, fields ) )
		
	#	fields = line.split( ',' )
		if nline == 1:
		#	line_header = line
			header_fields = fields
			nfields_header = len( fields )              # nfields of header line
			
			# need a list as result, otherwise error in comparison below: 
			# TypeError: 'itertools.imap' object has no attribute '__getitem__'
			csv_header_names = list( map( str.lower, fields ) )
			logging.info( "header fields: %s" % str( header_fields ) )
			logging.info( "# of csv header fields: %d" % nfields_header )
			ndiff = nfields_header - ncbg_names
			#logging.info( "ndiff: %d" % ndiff )
		
		else:
			for f, field in enumerate( fields ):
				logging.debug( "f: %d, field: %s" % ( f , field ) )
				nf = f + 1
				if field.count( '"' ) > 0:
					logging.error( "SPURIOUS double quote? nline: %d, num field: %d, field: %s" % ( nline, nf , field ) )
					logging.info( "line: %s" % ",".join( fields ) )
					logging.info( "fields: %s" % str( fields ) )
				
				if f >= nfields_header:
					logging.error( "OUTBOUND? nline: %d, num field: %d, field: %s" % ( nline, nf , field ) )
					logging.info( "line: %s" % ",".join( fields ) )
					logging.info( "fields: %s" % str( fields ) )
				else:
					header_field = header_fields[ f ]
				#	logging.info( "header_field: %s" % header_field )
				#	header_field = header_field.strip()
				#	header_field = header_field.strip( '"' )
					old_l = cbg_names[ header_field ]
					new_l = len( field )
					max_l = max( old_l, new_l )
					cbg_names[ header_field ] = max_l
			
			if use_limit:
				if nline >= max_lines:
					break
	
	csv_file_in.close()
	csv_file_out.close()
	
	nl_in = l + 1
	msg = "%6d lines read" % ( nl_in )
	print( msg ); logging.info( msg )
	
	msg = "%6d lines written" % ( nl_out )
	print( msg ); logging.info( msg )
	
	msg = "%6d lines skipped" % ( nl_skip )
	print( msg ); logging.info( msg )
	
	# found max field lengths
	logging.info( "max field lengths:" )
	for key, val in cbg_names.items():
		logging.info( "key: %s, val: %d" % ( key, val ) )



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
	log_file = True
	
	#log_level = logging.DEBUG
	log_level = logging.INFO
	#log_level = logging.WARNING
	#log_level = logging.ERROR
	#log_level = logging.CRITICAL
	
	if log_file:
		mode = 'w'
		#mode = 'a'	  # debugging
		logging_filename = "ingest_cbg_csv.log"
		logging.basicConfig( filename = logging_filename, filemode = mode, level = log_level )
	else:
		logging.basicConfig( level = log_level )

	time0 = time()		# seconds since the epoch
	logging.info( "start: %s" % datetime.datetime.now() )
	logging.info( __file__ )
	
	python_vertuple = sys.version_info
	python_version = str( python_vertuple[ 0 ] ) + '.' + str( python_vertuple[ 1 ] ) + '.' + str( python_vertuple[ 2 ] )
	logging.info( "Python version: %s" % python_version )

#	read_csv( CSV_DIR, CSV_GEB_in, CSV_GEB_out, cbg_birth_names )
#	read_csv( CSV_DIR, CSV_HUW_in, CSV_HUW_out, cbg_marriage_names )
	read_csv( CSV_DIR, CSV_OVL_in, CSV_OVL_out, cbg_death_names )


	logging.info( "stop: %s" % datetime.datetime.now() )
	str_elapsed = format_secs( time() - time0 )
	logging.info( "processing took %s" % str_elapsed )
	
	# This should be called at application exit,
	# and no further use of the logging system should be made after this call.
	logging.shutdown()
	


# [eof]
