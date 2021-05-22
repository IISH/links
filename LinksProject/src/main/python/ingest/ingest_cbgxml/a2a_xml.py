#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		a2a_xml.py
Version:	0.1
Goal:		A few helper function to process CBG A2A XMl files

22-May-2021 Created
22-May-2021 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import os
import sys

debug = False


def get_id_source_from_archive( db_ref, archive_name ):
	if debug: print( "get_id_source_from_archive()" )
	id_source  = None
	short_name = None
	
	query = "SELECT id_source, short_name FROM ref_source WHERE source_name = '%s'" % archive_name
	if debug: print( query )
	resp = db_ref.query( query )
	if resp is not None:
		#print( resp )
		nrec = len( resp )
		if nrec == 0:
			print( "No valid record found in ref_source for archive_name = '%s'" % archive_name )
		elif nrec == 1:
			rec = resp[ 0 ]
			id_source  = rec[ "id_source" ]
			short_name = rec[ "short_name" ]
		else:
			print( "Too many archive_name records found, ignoring them all\n" )
	
	if debug and id_source:
		print( "id_source = %d, short_name = %s" % ( id_source, short_name ) )
	
	return id_source, short_name
# get_id_source_from_archive()



def split_xml_fname( xml_fname ):
	if debug: print( "split_xml_fname()" )
	
	ghoe_type = None
	rmtype = None
	archive_name = ""
	
	root, ext = os.path.splitext( xml_fname )
	if ext != ".xml":
		print( "%s: xml_fname extension must be '.xml', but it is '%s'" % ( xml_fname, ext ) )
		return ghoe_type, rmtype, archive_name
	
	parts = root.split( '_' )
	
	if len( parts ) not in [ 4, 5 ]:	# date part: 4: yyyy-mm or 5: yyyy_mm
		print( "root: %s " % root )
		for p, part in enumerate ( parts ):
			print( "%d %s" % ( p, part ) )
		print( "%s: xml_fname must split into 4 parts, but it has %d" % ( xml_fname, len( parts ) ) )
		return ghoe_type, rmtype, archive_name
	
	prefix = archive_name = parts[ 0 ]
	if prefix != "A2A":
		print( "%s: prefix must be 'A2A', but it is '%s'" % ( xml_fname, prefix ) )
		return ghoe_type, rmtype, archive_name
	
	ghoe_type = parts[ 1 ]
	if ghoe_type == "BSG":
		rmtype = 1
	elif ghoe_type == "BSH":
		rmtype = 2
	elif ghoe_type == "BSO":
		rmtype = 3
	elif ghoe_type == "BSE":
		rmtype = 4
	else:
		print( "%s: ghoe_type must be one of 'BSG', 'BSH', 'BSE', 'BSO', but it is '%s'" % ( xml_fname, ghoe_type ) )
		return ghoe_type, rmtype, archive_name
	
	archive_name = parts[ -1 ]
	if debug: print( "archive_name: %s " % archive_name )

	return ghoe_type, rmtype, archive_name
# split_xml_fname()

# [eof]
