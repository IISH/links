#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS/CBG_PL_Conversion
Name:		CBG_PL_Convert.py
Version:	1.0
Goal:		Convert CBG data to PL csv file

17-Nov-2014	Created
26-Jul-2016	Python 2/3 compatibility
26-Jul-2016	Using chardet to determine input encoding
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import os
import csv			# However, the csv module does not support Unicode data; it certainly does not produce Unicode.
import chardet

from sys import exc_info, version_info

version_major = version_info.major
#print( "Python version:", version_major )
	
txt_dirname = os.path.dirname( os.path.realpath( __file__ ) )
csv_dirname = txt_dirname


def detect_encoding( path ):
	file = open( path, "rb" )
	data = file.read()
	file.close()
	
	print( "detecting input encoding..." )
	result = chardet.detect( data )
	
	enc  = result[ "encoding" ]
	conf = result[ "confidence" ] 
	print( "encoding: %s, confidence: %s" % ( enc, conf ) )
	
	return enc



def txt2csv( txt_filename ):
	try:
		txt_path = os.path.join( txt_dirname, txt_filename )
		enc = detect_encoding( txt_path )
		
		txt_file = open( txt_path, 'r', encoding = enc )
		#print( " input filename: %s" % txt_filename )
	except:
		etype, value, tb = exc_info()
		print( "open txt file failed: %s" % value )
		exit( 1 )

	try:
		basename, ext = os.path.splitext( txt_filename )
		#print( "basename:", basename )
		csv_filename = basename + ".csv"
		print( "output filename: %s" % csv_filename )
		csv_path     = os.path.join( csv_dirname, csv_filename )
		
		if version_major == 3:
			flags = 'w'
		else:
			flags = 'wb'		# 'w' for csv Python2 compat
		csv_file = open( csv_path, flags )
	except:
		etype, value, tb = exc_info()
		print( "open cvs file failed: %s" % value )
		exit( 1 )

	if version_major == 3:
		csv_writer = csv.writer( csv_file, delimiter = ',', quotechar = '|', quoting = csv.QUOTE_MINIMAL )		# Python3
	else:
		csv_writer = csv.writer( csv_file, delimiter = b',', quotechar = b'|', quoting = csv.QUOTE_MINIMAL )	# csv Python2 compat

	nline = 0
	nempty = 0

	while True:
		nline += 1
		line = txt_file.readline()			# includes trailing newline
		line = line.strip()					# remove newline
		size = len( line )

		if size == 0:
			break

	#	print( "%d (%d):  %s" % ( nline, size, line ) )

		fields = line.split( ';' )
		hsn_id = fields[ 0 ].strip()		# strip trailing spaces

		nfield = len( fields )
	#	print( "nfields: %d" % nfield )
	#	print( "%d %s %s" % ( 0, "HSN_ID", hsn_id ) )
	
		if version_major == 3:
			csv_writer.writerow( [ "HSN_ID", hsn_id ] )				# Python3 OK
		else:
			csv_writer.writerow( [ u"HSN_ID", unicode( hsn_id ) ] )	# Python2 compat

		for f in range( 1, nfield-1, 2 ):
			_id  = fields[ f ]
			
			if version_major == 3:
				_str = fields[ f+1 ]					# Python3 OK
			else:
				_str = fields[ f+1 ].encode( 'utf-8' )	# Python2 compat
		#	print( "%d %s %s" % ( f, _id, _str ) )
			csv_writer.writerow( [ _id, _str ] )
		
		csv_writer.writerow( [ '$' ] )		# KMs delimeter

	txt_file.close()
	csv_file.close()

	print( "%d lines processed" %  nline )



if __name__ == "__main__":
	print( "CBG_PL_Conversion" )
	txt_filename = input( "input filename: " )
	
	txt2csv( txt_filename )

# [eof]
