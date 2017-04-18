# -*- coding: utf-8 -*-

"""
FL-05-Apr-2017
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		militie_import.py
Version:	0.1
Goal:		Import miltieregister XML files into MySQL table

18-Apr-2017	Created
18-Apr-2017	Changed

/home/fons/projects/links/ingest/militieregisters/militie_import.py
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

from lxml import etree
from time import time

NM = "{http://Mindbus.nl/A2A}"
A2A_ID = 1						# not in XML

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



def xmlparse_store( es ):
	print( "\nxmlparse_store()" )

	len_nm = len( NM )
	section_names = [ "Person", "Event", "RelationEP", "Source" ]
	section_num  = 0
	person_num   = 0
	event_num    = 0
	relation_num = 0
	source_num   = 0

	section = None

	# both events used for section names, for others only start is used
	events = ( "start", "end" )

	for event, elem in etree.iterparse( xml_path, events ):
		tag_nm = elem.tag
		tag = tag_nm[ len_nm: ]
		print( "--- %s ---, %s" % ( tag, event ) )		# get section tags as the last

		if tag in section_names:		# section tag
			if event == "start":		# create new section object
				section_num += 1
			#	if section_num == 7:
			#		break

				if tag == "Person":
					pid = elem.get( "pid" )
					"""
					query = 'SELECT COUNT(*) FROM person_o_temp WHERE id_person_o="%s";' % pid
					resp = db.query( query )
					count_dict = resp[ 0 ]
					count = count_dict[ "COUNT(*)" ]
					print( "person count: %d" % count )
					if count > 0:
						print( "skipping existing person: %s\n" % pid )
					else:
						person_num  += 1
						section = Person( section_num, person_num, tag, pid )
					"""
					person_num  += 1
					section = Person( section_num, person_num, tag, pid )

				elif tag == "Event":
					eid = elem.get( "eid" )
					"""
					query = 'SELECT COUNT(*) FROM event WHERE eid="%s";' % eid
					resp = db.query( query )
					count_dict = resp[ 0 ]
					count = count_dict[ "COUNT(*)" ]
					print( "event count: %d" % count )
					if count > 0:
						print( "skipping existing event: %s\n" % eid )
					else:
						event_num += 1
						section = Event( section_num, event_num, tag, eid )
					"""
					event_num += 1
					section = Event( section_num, event_num, tag, eid )

				elif tag == "RelationEP":
					relation_num += 1
					a2a_id = A2A_ID
					section = RelationEP( section_num, relation_num, tag, a2a_id )

				elif tag == "Source":
					source_num += 1
					section = Source( section_num, source_num, tag )

				else:
					print( "unhandled tag: %s for: %s" % tag )
					exit( 1 )

				if section:
					print( "parse: section = %d, name = %s" % ( section_num, section.name ) )

			elif  event == "end" and section:		# if we have section object, store it
			#	print_section( section )
				store_data( es, section )
				del section
				print

		else:										# other tags
			if event == "start" and section:		# not for skipped (duplicate) sections
				#  Person tags
				if tag == "PersonNameFirstName":
					section.PersonName_PersonNameFirstName = elem.text
				elif tag == "PersonNamePatronym":
					section.PersonName_PersonNamePatronym = elem.text
				elif tag == "PersonNameLastName":
					section.PersonName_PersonNameLastName = elem.text
				elif tag == "Gender":
					section.Gender = elem.text
				elif tag == "Age":
					section.Age = elem.text
				elif tag == "PersonAgeLiteral":
					section.PersonAgeLiteral = elem.text
				elif tag == "Profession":
					section.Profession = elem.text

				#  Event tags
				elif tag == "EventType":
					section.EventType = elem.text
				elif tag == "Year":
					section.Year = elem.text
				elif tag == "Month":
					section.Month = elem.text
				elif tag == "Day":
					section.Day = elem.text

				# RelationEP tags
				elif tag == "RelationEP":
					section.relation = "RelationEP"
				elif tag == "PersonKeyRef":
					section.PersonKeyRef = elem.text
				elif tag == "EventKeyRef":
					section.EventKeyRef = elem.text
				elif tag == "RelationType":
					section.RelationType = elem.text

				# tags dependent on parent
				elif tag == "Place":
					parent = elem.getparent()
					if parent is None:
						print( "No parent?" )
						exit( 1 )
					parent_tag = parent.tag[ len_nm: ]
					if parent_tag == "SourcePlace":
						section.SourcePlace_Place = elem.text
					elif parent_tag == "SourceReference":
						section.SourceReference_Place = elem.text
					elif parent_tag == "EventPlace":
						section.EventPlace_Place = elem.text
					else:
						print( "unhandled parent tag: %s for: %s" % (parent_tag, tag) )
						exit( 1 )

				elif tag == "Province":
					parent = elem.getparent()
					parent_tag = parent.tag[ len_nm: ]
					if parent_tag == "EventPlace":
						section.EventPlace_Province = elem.text
					else:
						print( "unhandled parent tag: %s for: %s" % (parent_tag, tag) )
						exit( 1 )

				elif tag == "LiteralDate":
					parent = elem.getparent()
					parent_tag = parent.tag[ len_nm: ]
					if parent_tag == "BirthDate":
						section.BirthDate_LiteralDate = elem.text
					elif parent_tag == "EventDate":
						section.EventDate_LiteralDate = elem.text
					elif parent_tag == "SourceDate":
						section.SourceDate_LiteralDate = elem.text
					else:
						print( "unhandled parent tag: %s for: %s" % (parent_tag, tag) )
						exit( 1 )

				elif tag == "Year":
					parent = elem.getparent()
					parent_tag = parent.tag[ len_nm: ]
					if parent_tag == "BirthDate":
						section.BirthDate_Year = elem.text
					elif parent_tag == "EventDate":
						section.EventDate_Year = elem.text
					elif parent_tag == "SourceDate":
						section.SourceDate_Year = elem.text
					else:
						print( "unhandled parent tag: %s for: %s" % (parent_tag, tag) )
						exit( 1 )

				elif tag == "Month":
					parent = elem.getparent()
					parent_tag = parent.tag[ len_nm: ]
					if parent_tag == "BirthDate":
						section.BirthDate_Month = elem.text
					elif parent_tag == "EventDate":
						section.EventDate_Month = elem.text
					elif parent_tag == "SourceDate":
						section.SourceDate_Month = elem.text
					else:
						print( "unhandled parent tag: %s for: %s" % parent_tag, tag )
						exit( 1 )

				elif tag == "Day":
					parent = elem.getparent()
					parent_tag = parent.tag[ len_nm: ]
					if parent_tag == "BirthDate":
						section.BirthDate_Day = elem.text
					elif parent_tag == "EventDate":
						section.EventDate_Day = elem.text
					elif parent_tag == "SourceDate":
						section.SourceDate_Day = elem.text
					else:
						print( "unhandled parent tag: %s for: %s" % parent_tag, tag )
						exit( 1 )


				# Source tags
				elif tag == "From":
					section.SourceIndexDate_From = elem.text
				elif tag == "To":
					section.SourceIndexDate_To = elem.text
				elif tag == "SourceType":
					section.SourceType = elem.text
				elif tag == "InstitutionName":
					section.SourceReference_InstitutionName = elem.text
				elif tag == "Archive":
					section.SourceReference_Archive = elem.text
				elif tag == "RegistryNumber":
					section.SourceReference_RegistryNumber = elem.text
				elif tag == "DocumentNumber":
					section.SourceReference_DocumentNumber = elem.text
				elif tag == "OrderSequenceNumber":
					section.OrderSequenceNumber = elem.text
				elif tag == "UriViewer":
					section.UriViewer = elem.text
				elif tag == "UriPreview":
					section.UriPreview = elem.text
				elif tag == "SourceLastChangeDate":
					section.SourceLastChangeDate = elem.text
				elif tag == "RecordGUID":
					section.RecordGUID = elem.text
			#	elif tag == "Key":
			#		section.Key = elem.text
				elif tag == "Value":
					parent = elem.getparent()
					parent_tag = parent.tag[ len_nm: ]
					if parent_tag == "SourceRemark":
						key = parent.get( "Key" )
						print( "key: %s", key )
						if key == "AkteSoort":
							SourceRemark_AkteSoort = elem.text
						elif key == "Opmerking":
							SourceRemark_Opmerking = elem.text
						else:
							print( "unhandled attribute: %s for: %s" % key, parent_tag )
							exit( 1 )
					else:
						print( "unhandled parent tag: %s for: %s" % parent_tag, tag )
						exit( 1 )

				elif tag == "PersonName":
					pass
				elif tag == "BirthDate":
					pass
				elif tag == "EventDate":
					pass
				elif tag == "EventPlace":
					pass
				elif tag == "SourcePlace":
					pass
				elif tag == "SourceDate":
					pass
				elif tag == "SourceIndexDate":
					pass
				elif tag == "SourceReference":
					pass
				elif tag == "SourceRemark":
					pass
				else:
					print( "unhandled tag: %s" % tag )
					exit( 1 )

				#try:
				#	parent.clear()
				#except:
				#	pass

		# pertinent: delete the element when done processing, 
		# with clear() we can no longer determine the parent element
		if not ( tag == "SourcePlace" or "SourceReference" ):
			elem.clear()	# otherwise you still get the whole tree in memory



def process_xml( xml_pathname ):
	logging.debug( "process_xml()" )
	logging.info( "use: %s" % xml_pathname )




def process_batches():
	logging.debug( "process_batches()" )
	cur_dir = os.getcwd()
	batch_dir  = os.path.join( cur_dir, "batches" )
	dir_list = []
	if os.path.isdir( batch_dir ):
		dir_list = os.listdir( batch_dir )
		logging.info( "using batch directory: %s" % batch_dir )

	for filename in dir_list:
		root, ext = os.path.splitext( filename )
		if ext == ".xml":
			logging.info( "use: %s" % filename )
			xml_pathname = os.path.abspath( os.path.join( batch_dir, filename ) )
			process_xml( xml_pathname )
		else:
			logging.debuf( "skip: %s" % filename )



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
	log_level = logging.DEBUG
	#log_level = logging.INFO
	#log_level = logging.WARNING
	#log_level = logging.ERROR
	#log_level = logging.CRITICAL

	if log_file:
		logging_filename = "militie_import.log"
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
	
	process_batches()
	
	msg = "Stop: %s" % datetime.datetime.now()
	
	logging.info( msg )
	if log_file: print( msg )
	
	str_elapsed = format_secs( time() - time0 )
	print( "militie_import %s" % str_elapsed )	

# [eof]
