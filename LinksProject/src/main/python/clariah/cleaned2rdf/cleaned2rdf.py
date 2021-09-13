#!/usr/bin/env python
# coding: utf-8

"""
Author:	 Joe Raad, CLARIAH, Vrije Universiteit
Author:	 Fons Laan, KNAW HuC-DI & IISH - International Institute of Social History
Project: LINKS
Name:    cleaned2rdf.py
Version: 0.1
Goal:	 Create RDF from links_cleaned db selection

ToDo:	 - log file for each export
		 - check age_month, age_day usage in links
		 - optional merge rdf output
		 - report unused links fields from queries
		 - separate optional roles for marriage matchings

FL-02-Mar-2021 converted to script with jupyter from Joe's convert-births-to-rdf.ipynb and convert-marriages-to-rdf.ipynb
FL-09-Mar-2021 steering parameters in cleaned2rdf.yaml
FL-07-Apr-2021 cbg export of complete registration_c & person_c tables
FL-14-Apr-2021 encountered several '\' in registration_seq, which fucked up rdf2hdt conversion
"""

import datetime
import MySQLdb
import io
import os
import sys
import yaml

import pandas as pd
import math
import numpy as np
import glob
import collections
import re

#from datetime import datetime
from time import time


NAMESPACE = "https://iisg.amsterdam/"
DATASET_NAME = "links/"

PREFIX_IISG = NAMESPACE + DATASET_NAME
PREFIX_IISG_VOCAB = PREFIX_IISG + "vocab/"
PREFIX_SCHEMA = "http://schema.org/"
PREFIX_BIO = "http://purl.org/vocab/bio/0.1/"
PREFIX_FOAF = "http://xmlns.com/foaf/0.1/"
PREFIX_DC = "http://purl.org/dc/terms/"

TYPE_BIRTH_REGISTRATION = PREFIX_IISG_VOCAB + "BirthRegistration"
TYPE_BIRTH_EVENT = PREFIX_BIO + "Birth"
TYPE_MARRIAGE_REGISTRATION = PREFIX_IISG_VOCAB + "MarriageRegistration"
TYPE_MARRIAGE_EVENT = PREFIX_BIO + "Marriage"
TYPE_DEATH_REGISTRATION = PREFIX_IISG_VOCAB + "DeathRegistration"
TYPE_DEATH_EVENT = PREFIX_BIO + "Death"
TYPE_DIVORCE_REGISTRATION = PREFIX_IISG_VOCAB + "DivorceRegistration"
TYPE_DIVORCE_EVENT = PREFIX_BIO + "Divorce"

TYPE_PERSON = PREFIX_SCHEMA + "Person"
TYPE_PLACE = PREFIX_SCHEMA + "Place"
TYPE_COUNTRY = PREFIX_IISG + "Country"
TYPE_REGION = PREFIX_IISG + "Region"
TYPE_PROVINCE = PREFIX_IISG + "Province"
TYPE_MUNICIPALITY = PREFIX_IISG + "Municipality"

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"
REGISTER_EVENT = PREFIX_IISG_VOCAB + "registerEvent"
LOCATION = PREFIX_SCHEMA + "location"
DATE = PREFIX_BIO + "date"
REGISTRATION_ID = PREFIX_IISG_VOCAB + "registrationID"
REGISTRATION_SEQUENCE = PREFIX_IISG_VOCAB + "registrationSeqID"
BIRTH_DATE_FLAG = PREFIX_IISG_VOCAB + "birthDateFlag"

PERSON_ID = PREFIX_IISG_VOCAB + "personID"
GIVEN_NAME = PREFIX_SCHEMA + "givenName"
FAMILY_NAME = PREFIX_SCHEMA + "familyName"
GENDER = PREFIX_SCHEMA + "gender"
OCCUPATION = PREFIX_SCHEMA + "hasOccupation"
FAMILY_NAME_PREFIX = PREFIX_IISG_VOCAB + "prefixFamilyName"
AGE = PREFIX_FOAF + "age"
																# links role value
ROLE_NEWBORN          = PREFIX_IISG_VOCAB + "newborn"         # 1
ROLE_MOTHER           = PREFIX_IISG_VOCAB + "mother"          # 2
ROLE_FATHER           = PREFIX_IISG_VOCAB + "father"          # 3
ROLE_BRIDE            = PREFIX_IISG_VOCAB + "bride"           # 4
ROLE_BRIDE_MOTHER     = PREFIX_IISG_VOCAB + "motherBride"     # 5
ROLE_BRIDE_FATHER     = PREFIX_IISG_VOCAB + "fatherBride"     # 6
ROLE_GROOM            = PREFIX_IISG_VOCAB + "groom"           # 7
ROLE_GROOM_MOTHER     = PREFIX_IISG_VOCAB + "motherGroom"     # 8
ROLE_GROOM_FATHER     = PREFIX_IISG_VOCAB + "fatherGroom"     # 9
ROLE_DECEASED         = PREFIX_IISG_VOCAB + "deceased"        #	10
ROLE_DECEASED_PARTNER = PREFIX_IISG_VOCAB + "deceasedPartner" #	11
ROLE_UNKNOWN          = PREFIX_IISG_VOCAB + "unknown"         # else


def isNaN(num):
	if num == "\\N" or num == "":
		return True
	else:
		return False

def isNaN_Number(num):
	if num == 0:
		return True
	else:
		return False


def transformToInt(someNumber):
	return '"' + str(someNumber) + '"^^<http://www.w3.org/2001/XMLSchema#int>'

def transformToString(someWord):
	return '"' + str(someWord) + '"^^<http://www.w3.org/2001/XMLSchema#string>'

def transformToDate(someDate):
	return '"' + str(someDate) + '"^^<http://www.w3.org/2001/XMLSchema#date>'

def createQuad(s, p, o):
	return s + ' ' + p + ' ' + o + ' <https://iisg.amsterdam/links/births> .\n'


def getRole(role_number, registrationID):
	if str(role_number) == '1':
		return ROLE_NEWBORN
	if str(role_number) == '2':
		return ROLE_MOTHER
	if str(role_number) == '3':
		return ROLE_FATHER
	if str(role_number) == '4':
		return ROLE_BRIDE
	if str(role_number) == '5':
		return ROLE_BRIDE_MOTHER
	if str(role_number) == '6':
		return ROLE_BRIDE_FATHER
	if str(role_number) == '7':
		return ROLE_GROOM
	if str(role_number) == '8':
		return ROLE_GROOM_MOTHER
	if str(role_number) == '9':
		return ROLE_GROOM_FATHER
	if str(role_number) == '10':
		return ROLE_DECEASED
	if str(role_number) == '11':
		return ROLE_DECEASED_PARTNER
	else:
		print("Role number:", role_number, "- Certificate ID:", registrationID)
		return "UNKNOWN"


def createRegistrationURI(registrationID):
	return "<" + PREFIX_IISG + "registration/" + (str(registrationID)) + ">"

def createPersonURI(personID):
	return "<" + PREFIX_IISG + "person/" + (str(personID)) + ">"

def createEventURI(registrationID):
	return "<" + PREFIX_IISG + "event/" + (str(registrationID)) + ">"

def createTripleRegistrationID(registrationURI, registrationID):
	p = "<" + PREFIX_IISG_VOCAB + "registrationID" + ">"
	o = transformToInt(registrationID)
	return createQuad(registrationURI,p,o)

def createRegistrationTypeURI(registrationType):
	if registrationType == 'g':
		return "<" + PREFIX_IISG_VOCAB + "BirthRegistration" + ">"
	if registrationType == 'h':
		return "<" + PREFIX_IISG_VOCAB + "MarriageRegistration" + ">"
	if registrationType == 'o':
		return "<" + PREFIX_IISG_VOCAB + "DeathRegistration" + ">"
	if registrationType == 's':
		return "<" + PREFIX_IISG_VOCAB + "DivorceRegistration" + ">"
	else:
		return "<" + PREFIX_IISG_VOCAB + "MarriageRegistration" + ">"

def createTripleRegistrationType(registrationURI, registrationType):
	p = "<" + RDF_TYPE + ">"
	o = createRegistrationTypeURI(registrationType)
	if o != "EMTPY":
		return createQuad(registrationURI, p, o)
	else:
		print("Something is wrong", registrationURI, registrationType)

def createDate(year, month, day):
	fixedYear = str(year).zfill(4)
	fixedMonth = str(month).zfill(2)
	fixedDay = str(day).zfill(2)
	return transformToDate(fixedYear + "-" + fixedMonth + "-" + fixedDay)

def createTripleDate(registrationURI, fixedDate):
	p = "<" + DATE + ">"
	return createQuad(registrationURI, p , fixedDate)

def createLocationURI(locationID):
	return "<" + PREFIX_IISG + "place/" + str(locationID) + ">"

def createTripleLocation(registrationURI, locationID):
	p = "<" + LOCATION + ">"
	o = createLocationURI(locationID)
	return createQuad(registrationURI,p ,o)

def createTripleRegistrationSeq(registrationURI, registrationSeqID):
	p = "<" + PREFIX_IISG_VOCAB + "registrationSeqID" + ">"
	o = transformToString(registrationSeqID)
	return createQuad(registrationURI, p, o)

def createTripleRegistrationOrigID(registrationURI, registrationOrigID):
	p = "<" + PREFIX_IISG_VOCAB + "registrationOriginalID" + ">"
	o = transformToInt(registrationOrigID)
	return createQuad(registrationURI, p, o)

def createTripleLinksBase(registrationURI, not_linksbase):
	p = "<" + PREFIX_IISG_VOCAB + "linksBase" + ">"
	o = transformToString(not_linksbase)
	return createQuad(registrationURI, p, o)

def createTriplesRegisterEvent(registrationURI, eventURI):
	p = "<" + REGISTER_EVENT + ">"
	return createQuad(registrationURI, p, eventURI)

def createTriplePersonType(personURI):
	p = "<" + RDF_TYPE + ">"
	o = "<" + TYPE_PERSON + ">"
	return createQuad(personURI, p, o)

def createTriplePersonID(personURI, personID):
	p = "<" + PREFIX_IISG_VOCAB + "personID" + ">"
	o = transformToInt(personID)
	return createQuad(personURI, p, o)

def createTripleGender(personURI, gender):
	p = "<" + GENDER + ">"
	o = transformToString(gender)
	return createQuad(personURI, p, o)

def createTripleGivenName(personURI, givenName):
	p = "<" + GIVEN_NAME + ">"
	o = transformToString(givenName)
	return createQuad(personURI, p, o)

def createTripleFamilyName(personURI, familyName):
	p = "<" + FAMILY_NAME + ">"
	o = transformToString(familyName)
	return createQuad(personURI, p, o)

def createTriplePrefix(personURI, prefix):
	p = "<" + FAMILY_NAME_PREFIX + ">"
	o = transformToString(prefix)
	return createQuad(personURI, p, o)

def createTripleOccupation(personURI, occupation):
	p = "<" + OCCUPATION + ">"
	o = transformToString(occupation)
	return createQuad(personURI, p, o)

def createTripleAge(personURI, age):
	p = "<" + AGE + ">"
	o = transformToInt(age)
	return createQuad(personURI, p, o)

def createTripleRole(eventURI, normalisedRole, personURI):
	p = "<" + normalisedRole + ">"
	return createQuad(eventURI, p, personURI)



def convertCSVRegistrationsToRDF(inputData, outputData):
	print("Converting Registrations CSV file to RDF...")
	start_time = datetime.datetime.now() 
	f = open(outputData,"w+")
	ch_size = 10000

	#df_chunk = pd.read_csv(inputData, chunksize=ch_size, low_memory=False, keep_default_na=False)
	delimiter = ';'
	quotechar = '"'
	df_chunk = pd.read_csv(inputData, quotechar=quotechar, delimiter=delimiter, chunksize=ch_size, low_memory=False, keep_default_na=False)

	counter = 0
	for chunk in df_chunk:
		print("# " + str(counter) + " rows")
		counter = counter + ch_size
		filebuffer = []
		for index, row in chunk.iterrows():
			registrationID = row['id_registration']
			registrationURI = createRegistrationURI(registrationID)
			if not isNaN(registrationID):
				filebuffer.append(createTripleRegistrationID(registrationURI, registrationID))
				registrationType = row['registration_type']
				if not isNaN(registrationType):
					filebuffer.append(createTripleRegistrationType(registrationURI, registrationType))
				year = row['registration_year']
				month = row['registration_month']
				day = row['registration_day']
				if not isNaN_Number(year) and not isNaN_Number(month) and not isNaN_Number(day):
					if not isNaN(year) and not isNaN(month) and not isNaN(day):
						fixedDate = createDate(year, month, day)
						filebuffer.append(createTripleDate(registrationURI, fixedDate))
				locationID = row['registration_location_no']
				if not isNaN(locationID):
					filebuffer.append(createTripleLocation(registrationURI, locationID))
				registrationOrigID = row['id_orig_registration']
				if not isNaN(registrationOrigID):
					filebuffer.append(createTripleRegistrationOrigID(registrationURI, registrationOrigID))
				registrationSeqID = row['registration_seq']
				if not isNaN(registrationSeqID):
					filebuffer.append(createTripleRegistrationSeq(registrationURI, registrationSeqID))
				not_linksbase = row['not_linksbase']
				if not isNaN(not_linksbase):
					filebuffer.append(createTripleLinksBase(registrationURI, not_linksbase))
		f.writelines(filebuffer)
	f.close()
	print("Program Finished!")
	time_elapsed = datetime.datetime.now() - start_time 
	print('Time elapsed (hh:mm:ss) {}'.format(time_elapsed))
# convertCSVRegistrationsToRDF()



def convertCSVPersonsToRDF(inputData, outputData):
	print("Converting Persons CSV file to RDF...")
	start_time = datetime.datetime.now() 
	f = open(outputData,"w+")
	ch_size = 10000

	#df_chunk = pd.read_csv(inputData, chunksize=ch_size, low_memory=False, error_bad_lines=False, keep_default_na=False)
	delimiter = ';'
	quotechar = '"'
	df_chunk = pd.read_csv(inputData, quotechar=quotechar, delimiter=delimiter, chunksize=ch_size, low_memory=False, keep_default_na=False)

	counter = 0
	for chunk in df_chunk:
		print("# " + str(counter) + " rows")
		counter = counter + ch_size
		filebuffer = []
		for index, row in chunk.iterrows():
			personID = row['id_person']
			registrationID = row['id_registration']
			if not isNaN(personID) and not isNaN(registrationID):
				personURI = createPersonURI(personID)
				registrationURI = createRegistrationURI(registrationID)
				eventURI = createEventURI(registrationID)        
				filebuffer.append(createTriplePersonType(personURI))
				filebuffer.append(createTriplePersonID(personURI, personID))                    
				givenName = row['firstname']
				if not isNaN(givenName):
					filebuffer.append(createTripleGivenName(personURI, givenName))
				familyName = row['familyname']
				if not isNaN(familyName):
					filebuffer.append(createTripleFamilyName(personURI, familyName))
				prefix = row['prefix']
				if not isNaN(prefix):
					filebuffer.append(createTriplePrefix(personURI, prefix))    
				gender = row['sex']
				if not isNaN(gender):
					filebuffer.append(createTripleGender(personURI, gender))
				occupation = row['occupation']
				if not isNaN(occupation):
					occupationFixed = re.sub('[^A-Za-z0-9 ]+', '', occupation)
					filebuffer.append(createTripleOccupation(personURI, occupationFixed))
				age = row['age_year']
				if not isNaN(age):
					filebuffer.append(createTripleAge(personURI, age))
				
				role = row['role']
				if not isNaN(role):
					normalisedRole = getRole(role, registrationID)
					if normalisedRole != "UNKNOWN":
						filebuffer.append(createTripleRole(eventURI, normalisedRole, personURI))
					main = False
					if normalisedRole == ROLE_NEWBORN:
						main  = True
						year  = row['birth_year']
						month = row['birth_month']
						day   = row['birth_day']
						locationID = row['birth_location']
					if normalisedRole == ROLE_BRIDE:
						main  = True
						year  = row['mar_year']
						month = row['mar_month']
						day   = row['mar_day']
						locationID = row['mar_location']
					if normalisedRole == ROLE_DECEASED:
						main  = True
						year  = row['death_year']
						month = row['death_month']
						day   = row['death_day']
						locationID = row['death_location']
					if main == True:
						filebuffer.append(createTriplesRegisterEvent(registrationURI, eventURI))
						if not isNaN_Number(year) and not isNaN_Number(month) and not isNaN_Number(day):
							if not isNaN(year) and not isNaN(month) and not isNaN(day):
								fixedDate = createDate(year, month, day)
								filebuffer.append(createTripleDate(eventURI, fixedDate))
						if not isNaN(locationID):
							filebuffer.append(createTripleLocation(eventURI, locationID))
		f.writelines(filebuffer)
	f.close()
	print("Program Finished!")
	time_elapsed = datetime.datetime.now() - start_time 
	print('Time elapsed (hh:mm:ss) {}'.format(time_elapsed))
# convertCSVPersonsToRDF()



def convertTableRegistrationsToRDF( db, query, rdf_path ):
	start_time = datetime.datetime.now()
	
	print( "Converting table registration_c to RDF..." )
	
	print( rdf_path )
	rdf_file = open( rdf_path, "w" )

	print( query )
	resp = db.query( query )
	
	if resp is None:
		print( "No query result, Nothing to do!" )
		return
	elif len( resp ) == 0:
		print( "No records, Nothing to do!" )
		return
	else:
		print( "processing %d links records" % len( resp ) )
		
		counter  = 0
		nchunk   = 0
		ntriples = 0
		chunk_size = 10000
		filebuffer = []
		
		for r, row in enumerate( resp ):
			#print( r, row )
			
			registrationID  = row[ "id_registration" ]
			registrationURI = createRegistrationURI( registrationID )
			if not isNaN( registrationID ):
				filebuffer.append( createTripleRegistrationID( registrationURI, registrationID ) )
				
				registrationType = none2empty( row ["registration_type" ] )
				if not isNaN( registrationType ):
					filebuffer.append( createTripleRegistrationType( registrationURI, registrationType ) )
				
				year  = none2empty( row[ "registration_year" ] )
				month = none2empty( row[ "registration_month" ] )
				day   = none2empty( row[ "registration_day" ] )
				if not isNaN_Number( year ) and not isNaN_Number( month ) and not isNaN_Number( day ):
					if not isNaN( year ) and not isNaN( month ) and not isNaN( day ):
						fixedDate = createDate( year, month, day )
						filebuffer.append( createTripleDate( registrationURI, fixedDate ) )
				
				locationID = none2empty( row[ "registration_location_no"] )
				if not isNaN( locationID ):
					filebuffer.append( createTripleLocation( registrationURI, locationID ) )
				
				registrationOrigID = none2empty( row[ "id_orig_registration" ] )
				if not isNaN( registrationOrigID ):
					filebuffer.append( createTripleRegistrationOrigID( registrationURI, registrationOrigID ) )
				
				registrationSeqID = none2empty( row[ "registration_seq" ] )
				registrationSeqID = registrationSeqID.replace( "\\", '' )	# we encountered several registration_seq that started 
				# with a '\', and that gave "Illegal escape sequence value" by the rdf2hdt conversion. 
				if not isNaN( registrationSeqID ):
					filebuffer.append( createTripleRegistrationSeq( registrationURI, registrationSeqID ) )
				
				not_linksbase = none2empty( row[ "not_linksbase" ] )
				if not isNaN( not_linksbase ):
					filebuffer.append( createTripleLinksBase( registrationURI, not_linksbase ) )
			
			if r > 0 and r % chunk_size == 0:
				nchunk += 1
				ntriples += chunk_size
				print( "write chunk %d (%d)" % ( nchunk, r ) )
				rdf_file.writelines( filebuffer )
				filebuffer = []
		
		nremains = len( filebuffer )
		if nremains > 0:
			ntriples += nremains
			rdf_file.writelines( filebuffer )
			print( "write remains (%d)" % (r+1) )
		
		print( "%d links records processed" % (r+1) )
		print( "%d N-Quads written" % ntriples )

	rdf_file.close()
	
	print( "Program Finished!" )
	time_elapsed = datetime.datetime.now() - start_time 
	print( 'Time elapsed (hh:mm:ss) {}'.format( time_elapsed ) )
#convertTableRegistrationsToRDF()



def convertTablePersonsToRDF( db, query, rdf_path ):
	start_time = datetime.datetime.now()
	
	print( "Converting table person_c to RDF..." )
	
	print( rdf_path )
	rdf_file = open( rdf_path, "w" )

	print( query )
	resp = db.query( query )
	
	if resp is None:
		print( "No query result, Nothing to do!" )
		return
	elif len( resp ) == 0:
		print( "No records, Nothing to do!" )
		return
	else:
		print( "processing %d links records" % len( resp ) )
		
		counter  = 0
		nchunk   = 0
		ntriples = 0
		chunk_size = 10000
		filebuffer = []
		
		for r, row in enumerate( resp ):
			#print( r, row )
			
			personID = row[ "id_person" ]
			registrationID = row[ "id_registration" ]
			if not isNaN( personID ) and not isNaN( registrationID ):
				personURI = createPersonURI( personID )
				registrationURI = createRegistrationURI( registrationID )
				eventURI = createEventURI( registrationID )
				filebuffer.append( createTriplePersonType( personURI ) )
				filebuffer.append( createTriplePersonID( personURI, personID ) )
				
				givenName = none2empty( row[ "firstname" ] )
				if not isNaN( givenName ):
					filebuffer.append( createTripleGivenName( personURI, givenName ) )
				
				familyName = none2empty( row[ "familyname" ] )
				if not isNaN( familyName ):
					filebuffer.append( createTripleFamilyName( personURI, familyName ) )
				
				prefix = none2empty( row[ "prefix" ] )
				if not isNaN( prefix ):
					filebuffer.append( createTriplePrefix( personURI, prefix ) )
				
				gender = none2empty( row[ "sex" ] )
				if not isNaN( gender ):
					filebuffer.append( createTripleGender( personURI, gender ) )
				
				occupation = none2empty( row[ "occupation" ] )
				if not isNaN( occupation ):
					occupationFixed = re.sub( '[^A-Za-z0-9 ]+', '', occupation )
					filebuffer.append( createTripleOccupation( personURI, occupationFixed ) )
				
				age = none2empty( row[ "age_year" ] )
				if not isNaN( age ):
					filebuffer.append( createTripleAge( personURI, age ) )
				
				role = none2empty( row[ "role" ] )
				if not isNaN( role ):
					normalisedRole = getRole( role, registrationID )
					if normalisedRole != "UNKNOWN":
						filebuffer.append( createTripleRole( eventURI, normalisedRole, personURI ) )
					
					main = False
					if normalisedRole == ROLE_NEWBORN:
						main  = True
						year  = none2empty( row[ "birth_year" ] )
						month = none2empty( row[ "birth_month" ] )
						day   = none2empty( row[ "birth_day" ] )
						locationID = none2empty( row[ "birth_location" ] )
					elif normalisedRole == ROLE_BRIDE:
						main  = True
						year  = none2empty( row[ "mar_year" ] )
						month = none2empty( row[ "mar_month" ] )
						day   = none2empty( row[ "mar_day" ] )
						locationID = none2empty( row[ "mar_location" ] )
					elif normalisedRole == ROLE_DECEASED:
						main  = True
						year  = none2empty( row[ "death_year" ] )
						month = none2empty( row[ "death_month" ] )
						day   = none2empty( row[ "death_day" ] )
						locationID = none2empty( row["death_location" ] )
					
					if main == True:
						filebuffer.append( createTriplesRegisterEvent( registrationURI, eventURI ) )
						
						if not isNaN_Number( year ) and not isNaN_Number( month ) and not isNaN_Number( day ):
							if not isNaN( year ) and not isNaN( month ) and not isNaN( day ):
								fixedDate = createDate( year, month, day )
								filebuffer.append( createTripleDate( eventURI, fixedDate ) )
						
						if not isNaN( locationID ):
							filebuffer.append( createTripleLocation( eventURI, locationID ) )
				
				#not_linksbase_p = none2empty( row[ "not_linksbase_p" ] )
				#if not isNaN( not_linksbase_p ):
				#	filebuffer.append( createTripleLinksBase( registrationURI, not_linksbase_p ) )
				
			if r > 0 and r % chunk_size == 0:
				nchunk += 1
				ntriples += chunk_size
				print( "write chunk %d (%d)" % ( nchunk, r ) )
				rdf_file.writelines( filebuffer )
				filebuffer = []
		
		nremains = len( filebuffer )
		if nremains > 0:
			ntriples += nremains
			rdf_file.writelines( filebuffer )
			print( "write remains (%d)" % (r+1) )
		
		print( "%d links records processed" % (r+1) )
		print( "%d N-Quads written" % ntriples )

	rdf_file.close()
	
	print( "Program Finished!" )
	time_elapsed = datetime.datetime.now() - start_time 
	print( 'Time elapsed (hh:mm:ss) {}'.format( time_elapsed ) )
# convertTablePersonsToRDF()



def make_queries( match_params, config_params ):
	print( "make_queries()" )

	s1_rmtype = match_params[ "s1_rmtype" ]
	s1_rtype  = match_params[ "s1_rtype" ]
	s1_roles  = match_params[ "s1_roles" ]
	
	s2_rmtype = match_params[ "s2_rmtype" ]
	s2_rtype  = match_params[ "s2_rtype" ]
	s2_roles  = match_params[ "s2_roles" ]
	
	id_sources = config_params[ "id_sources" ]

	s1_person_fields = config_params[ "person_fields_common" ]
	
	if s1_rmtype == 1 and s1_rtype == 'g':
		s1_person_fields += ", %s" % config_params[ "person_fields_birth" ]
	elif s1_rmtype == 2 and s1_rtype == 'h':
		s1_person_fields += ", %s" % config_params[ "person_fields_marriage" ]
	elif s1_rmtype == 3 and s1_rtype == 'o':
		s1_person_fields += ", %s" % config_params[ "person_fields_death" ]
	
	s2_person_fields = config_params[ "person_fields_common" ]
	
	if s2_rmtype == 1 and s2_rtype == 'g':
		s2_person_fields += ", %s" % config_params[ "person_fields_birth" ]
	elif s2_rmtype == 2 and s2_rtype == 'h':
		s2_person_fields += ", %s" % config_params[ "person_fields_marriage" ]
	elif s2_rmtype == 3 and s2_rtype == 'o':
		s2_person_fields += ", %s" % config_params[ "person_fields_death" ]
	
	
	s1_query_registration  = "SELECT %s FROM links_cleaned.registration_c" % config_params[ "registration_fields" ]
	s1_query_registration += " WHERE registration_maintype = %d" % s1_rmtype
	s1_query_registration += " AND registration_type = '%s'" % s1_rtype
	s1_query_registration += " AND not_linksbase IS NULL"
	if id_sources:
		s1_query_registration += " AND id_source IN (%s)" % id_sources
	
	s1_query_person  = "SELECT %s FROM links_cleaned.person_c" % s1_person_fields
	s1_query_person += " WHERE registration_maintype = %d" % s1_rmtype
	s1_query_person += " AND registration_type = '%s'" % s1_rtype
	s1_query_person += " AND role IN (%s)" % s1_roles
	s1_query_person += " AND not_linksbase_p IS NULL"
	if id_sources:
		s1_query_person += " AND id_source IN (%s)" % id_sources
	
	s2_query_registration  = "SELECT %s FROM links_cleaned.registration_c" % config_params[ "registration_fields" ]
	s2_query_registration += " WHERE registration_maintype = %d" % s2_rmtype
	s2_query_registration += " AND registration_type = '%s'" % s2_rtype
	s2_query_registration += " AND not_linksbase IS NULL"
	if id_sources:
		s2_query_registration += " AND id_source IN (%s)" % id_sources
	s2_query_registration.strip()

	
	s2_query_person  = "SELECT %s FROM links_cleaned.person_c" % s2_person_fields
	s2_query_person += " WHERE registration_maintype = %d" % s2_rmtype
	s2_query_person += " AND registration_type = '%s'" % s2_rtype
	s2_query_person += " AND role IN (%s)" % s2_roles
	s2_query_person += " AND not_linksbase_p IS NULL"
	if id_sources:
		s2_query_person += " AND id_source IN (%s)" % id_sources
	
	# clean whitespace
	s1_query_registration = ' '.join( s1_query_registration.split() )
	s2_query_registration = ' '.join( s2_query_registration.split() )
	s1_query_person = ' '.join( s1_query_person.split() )
	s2_query_person = ' '.join( s2_query_person.split() )
	
	print( "s1_query_registration: %s " %  s1_query_registration)
	print( "s1_query_person: %s" % s1_query_person )
	print( "s2_query_registration: %s" % s2_query_registration )
	print( "s2_query_person: %s" % s2_query_person )
	
	return s1_query_registration, s1_query_person, s2_query_registration, s2_query_person
# make_queries()



def get_matches_params():
	print( "set_match_params()" )
	
	"""
	Showing the relation of Joe matching types with Kees LINKS parameters in match_process table. 
	9 matching types, 3 between generations, 6 within a generation.
	The role #'s of the parents in [].
	========================================================================================
	#	Done	Name		s1_r[m]type		s1_role		s2_r[m]type		s2_role		LINKS
	----------------------------------------------------------------------------------------
	1	 y	 Within_B_M			1	g		1 [,2,3]		2	h		4,7			
	2	 y	Between_M_M			2	h		5,6,8,9			2	h		4,7			released
	3	 y	Between_B_M			1	g		2,3				2	h		4,7			released
	----------------------------------------------------------------------------------------
	4	 n	 Within_B_D			1	g		1 [,2,3			3	o		10 [2,3,]	
	5	 n	 Within_M_D			2	h		4,7 [,5,6,8,9]	3	o		10,11 [,2,3]
	6	 n	 Within_B_B			1	g		[2,3]			1	g		[2,3]		
	7	 n	 Within_M_M			2	h		5,6,8,9			2	h		5,6,8,9		
	8	 n	 Within_D_D			3	o		2,3				3	o		2,3			
	9	 n	Between_D_M			3	o		10,11 [,2,3]	2	h		4,7 [,5,6,8,9]
	========================================================================================
	
	Role #'s of the 2 generations in the certificates
	================+============================
					|---------Certificate--------
					| Birth		Marriage	Death
	----------------+----------------------------
	Role Prev Gen	| 2,3		5,6,8,9		2,3	
	Role Curr Gen	| 1			4,7			10,11
	================+============================
	
	======================
					role #
	----------------------
	newborn"			 1
	mother"				 2
	father"				 3
	bride"				 4
	motherBride"		 5
	fatherBride"		 6
	groom"				 7
	motherGroom"		 8
	fatherGroom"		 9
	deceased"			10
	deceasedPartner"	11
	unknown"			
==========================
"""
	
	matches_params = {
		# 1	y	1	g	1,2,3			2	h	4,7
		"Within_B_M" : {
			"s1_rmtype" : 1,
			"s1_rtype"  : 'g',
			"s1_roles"  : "1,2,3",
			"s2_rmtype" : 2,
			"s2_rtype"  : 'h',
			"s2_roles"  : "4,7"
		},
		
		# 2	y	2	h	5,6,8,9			2	h	4,7
		"Between_M_M" : {
			"s1_rmtype" : 2,
			"s1_rtype"  : 'h',
			"s1_roles"  : "5,6,8,9",
			"s2_rmtype" : 2,
			"s2_rtype"  : 'h',
			"s2_roles"  : "4,7"
		},
		
		# 3	 y	1	g	2,3				2	h	4,7
		"Between_B_M" : { 
			"s1_rmtype" : 1,
			"s1_rtype"  : 'g',
			"s1_roles"  : "2,3",
			"s2_rmtype" : 2,
			"s2_rtype"  : 'h',
			"s2_roles"  : "4,7"
		},
		
		# 4	 n	1	g	1,2,3			3	o	2,3,10
		"Within_B_D" : {
			"s1_rmtype" : 1,
			"s1_rtype"  : 'g',
			"s1_roles"  : "1,2,3",
			"s2_rmtype" : 3,
			"s2_rtype"  : 'o',
			"s2_roles"  : "2,3,10"
		},
		# 5	 n	2	h	4,5,6,7,8,9		3	o		2,3,10,11
		"Within_M_D": {
			"s1_rmtype" : 2,
			"s1_rtype"  : 'h',
			"s1_roles"  : "4,5,6,7,8,9",
			"s2_rmtype" : 3,
			"s2_rtype"  : 'o',
			"s2_roles"  : "2,3,10,11"
		},
		# 6	 n	1	g	2,3				1	g		2,3
		"Within_B_B": {
			"s1_rmtype" : 1,
			"s1_rtype"  : 'g',
			"s1_roles"  : "2,3",
			"s2_rmtype" : 1,
			"s2_rtype"  : 'g',
			"s2_roles"  : "2,3"
		},
		# 7	 n	2	h	5,6,8,9			2	h		5,6,8,9
		"Within_M_M": {
			"s1_rmtype" : 2,
			"s1_rtype"  : 'h',
			"s1_roles"  : "5,6,8,9",
			"s2_rmtype" : 2,
			"s2_rtype"  : 'h',
			"s2_roles"  : "5,6,8,9"
		},
		# 8	 n	3	o	2,3				3	o		2,3
		"Within_D_D": {
			"s1_rmtype" : 3,
			"s1_rtype"  : 'o',
			"s1_roles"  : "2,3",
			"s2_rmtype" : 3,
			"s2_rtype"  : 'o',
			"s2_roles"  : "2,3"
		},
		# 9	 n	3	o	2,3,10,11		2	h		4,5,6,7,8,9
		"Between_D_M": {
			"s1_rmtype" : 3,
			"s1_rtype"  : 'o',
			"s1_roles"  : "2,3,10,11",
			"s2_rmtype" : 2,
			"s2_rtype"  : 'h',
			"s2_roles"  : "4,5,6,7,8,9"
		}
	}
	
	return matches_params
# get_matches_params()



def make_rdf_by_function( rdf_dir, match_params, config_params ):
	print( "make_rdf_by_function()" )
	
	s1_query_registration, s1_query_person, s2_query_registration, s2_query_person = make_queries( match_params, config_params )
	
	date = datetime.datetime.now().strftime( "%04Y.%02m.%02d" )
	
	s1_sample_spec = ""
	s2_sample_spec = ""
	s1_certificate_spec = ""
	s2_certificate_spec = ""
	
	rdf_merge = config_params[ "rdf_merge" ]
	if rdf_merge in [ "", "None" ]:
		s1_sample_spec     = "-s1"
		s2_sample_spec     = "-s2"
		r_certificate_spec = "-registrations"
		p_certificate_spec = "-persons"
	elif rdf_merge == "Samples":
		r_certificate_spec = "-registrations"
		p_certificate_spec = "-persons"
	elif rdf_merge == "Certificates":
		s1_sample_spec     = "-s1"
		s2_sample_spec     = "-s2"
	
	source_spec = ""
	source_name = config_params[ "source_name" ]
	if source_name:
		source_spec = "-source=%s" % source_name
	
	if not source_spec:
		id_sources  = config_params[ "id_sources" ]
		sources = ''.join( id_sources.split() )
		source_spec = "-source=%s" % sources
	
	s1_registration_rdf_name = "match=%s%s%s%s-%s.nq" % ( match_function, s1_sample_spec, r_certificate_spec, source_spec, date )
	s2_registration_rdf_name = "match=%s%s%s%s-%s.nq" % ( match_function, s2_sample_spec, r_certificate_spec, source_spec, date )
	
	s1_registration_rdf_path = os.path.join( rdf_dir, s1_registration_rdf_name )
	s2_registration_rdf_path = os.path.join( rdf_dir, s2_registration_rdf_name )
	
	convertTableRegistrationsToRDF( db, s1_query_registration, s1_registration_rdf_path )
	convertTableRegistrationsToRDF( db, s2_query_registration, s2_registration_rdf_path )
	
	s1_person_rdf_name = "match=%s%s%s%s-%s.nq" % ( match_function, s1_sample_spec, p_certificate_spec, source_spec, date  )
	s2_person_rdf_name = "match=%s%s%s%s-%s.nq" % ( match_function, s2_sample_spec, p_certificate_spec, source_spec, date  )
	
	s1_person_rdf_path = os.path.join( rdf_dir, s1_person_rdf_name )
	s2_person_rdf_path = os.path.join( rdf_dir, s2_person_rdf_name )
	
	convertTablePersonsToRDF( db, s1_query_person, s1_person_rdf_path )
	convertTablePersonsToRDF( db, s2_query_person, s2_person_rdf_path )
	
# make_rdf_by_function()



def make_rdf_cbg( db, rdf_dir, config_params ):
	print( "make_rdf_cbg()" )
	
	date = datetime.datetime.now().strftime( "%04Y.%02m.%02d" )
	
	source_spec = ""
	source_name = config_params[ "source_name" ]
	if source_name:
		source_spec = "-%s" % source_name
	
	if not source_spec:
		id_sources  = config_params.get( "id_sources", "" )
		if id_sources:
			sources = ''.join( id_sources.split() )
			source_spec = "-source=%s" % sources
	
	
	# registrations
	registration_fields = config_params[ "registration_fields" ]
	
	rmtypes  = "1,2,3"
	rtypes   = "'g','h','o'"
	role_min = 1
	role_max = 11
	
	query_registration  = "SELECT %s FROM links_cleaned.registration_c" % registration_fields
	query_registration += " WHERE registration_maintype IN (%s)" % rmtypes
	query_registration += " AND registration_type IN (%s)" % rtypes
	query_registration += " AND not_linksbase IS NULL"
	query_registration += " AND id_persist_registration IS NOT NULL AND id_persist_registration <> ''"
	if id_sources:
		query_registration += " AND id_source IN (%s)" % id_sources
	query_registration = ' '.join( query_registration.split() )	# clean whitespace
	#print( "query_registration: %s " % query_registration)
	
	r_prefix = "cbg-registrations"
	registration_rdf_name = "%s%s-%s.nq" % ( r_prefix, source_spec, date )
	registration_rdf_path = os.path.join( rdf_dir, registration_rdf_name )
	convertTableRegistrationsToRDF( db, query_registration, registration_rdf_path )
	
	"""
	# persons
	# doing person_c in one go used to much memory (> 50 GB, Killed by kernel)
	# consider using LIMIT or fetchmany() or add-on module pymysql_utils
	# tmp solution: split into birth/marriage/death
	"""
	person_fields = config_params[ "person_fields_common" ]
	person_fields += ", %s" % config_params[ "person_fields_birth" ]
	person_fields += ", %s" % config_params[ "person_fields_marriage" ]
	person_fields += ", %s" % config_params[ "person_fields_death" ]
	
	query_person  = "SELECT %s FROM links_cleaned.person_c" % person_fields
	query_person += " WHERE registration_maintype IN (%s)" % rmtypes
	query_person += " AND registration_type IN (%s)" % rtypes
	query_person += " AND role >= %s AND role <= %s" % ( role_min, role_max )
	query_person += " AND not_linksbase_p IS NULL"
	if id_sources:
		query_person += " AND id_source IN (%s)" % id_sources
	query_person = ' '.join( query_person.split() )		# clean whitespace
	#print( "query_person: %s" % query_person )
	"""
	
	person_fields_common   = config_params[ "person_fields_common" ]
	person_fields_birth    = config_params[ "person_fields_birth" ]
	person_fields_marriage = config_params[ "person_fields_marriage" ]
	person_fields_death    = config_params[ "person_fields_death" ]
	
	for registration_maintype in [ 1, 2, 3 ]:
		if registration_maintype == 1:
			person_fields  = person_fields_common
			person_fields += ", %s" % person_fields_birth
			registration_type = 'g'
			roles = "1,2,3"
			p_prefix = "cbg-persons-birth"
			
		elif registration_maintype == 2:
			person_fields  = person_fields_common
			person_fields += ", %s" % person_fields_marriage
			registration_type = 'h'
			roles = "4,5,6,7,8,9"
			p_prefix = "cbg-persons-marriage"
			
		elif registration_maintype == 3:
			person_fields  = person_fields_common
			person_fields += ", %s" % person_fields_death
			registration_type = 'o'
			roles = "2,3,10,11"
			p_prefix = "cbg-persons-death"
	
		query_person  = "SELECT %s FROM links_cleaned.person_c" % person_fields
		query_person += " WHERE registration_maintype = %d" % registration_maintype
		query_person += " AND registration_type = '%s'" % registration_type
		query_person += " AND role IN (%s)" % roles
		query_person += " AND not_linksbase_p IS NULL"
		
		if id_sources:
			query_person += " AND id_source IN (%s)" % id_sources
			query_person = ' '.join( query_person.split() )		# clean whitespace
		
		print( "query_person: %s" % query_person )
		
		person_rdf_name = "%s%s-%s.nq" % ( p_prefix, source_spec, date )
		person_rdf_path = os.path.join( rdf_dir, person_rdf_name )
		convertTablePersonsToRDF( db, query_person, person_rdf_path )
		"""
# make_rdf_cbg()



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
# get_yaml_config()



if __name__ == "__main__":
	time0 = time()		# seconds since the epoch
	msg = "Start: %s" % datetime.datetime.now()

	python_vertuple = sys.version_info
	python_version = str( python_vertuple[ 0 ] ) + '.' + str( python_vertuple[ 1 ] ) + '.' + str( python_vertuple[ 2 ] )
	print( "Python version: %s" % python_version )

	script_name = "cleaned2rdf"
	print( "%s.py" % script_name )

	yaml_filename = "./%s.yaml" % script_name
	config_local = get_yaml_config( yaml_filename )

	cur_dir = os.path.abspath( os.path.dirname( "__file__"  ) )
	rdf_dir = os.path.join( cur_dir, "rdf" )
	
	if not os.path.exists( rdf_dir ):
		os.makedirs( rdf_dir )

#	from_source = "file"
	from_source = "table"

	if from_source == "file":
		registrations_csv_path = "csv/joe_links_cleaned.registration_c-source=231,242-rmtype=1-rtype=g-2021.02.26.csv"
		output_file_registrations = os.path.join( rdf_dir, "births-registrations-2021.02.26.nq" )
		convertCSVRegistrationsToRDF( registrations_csv_path, output_file_registrations )
	
		persons_csv_path = "csv/joe_links_cleaned.person_c-id_source=231,242-rm_type=1-rtype=g-2021.02.26.csv"
		output_file_persons = os.path.join( rdf_dir, "births-persons-2021.02.26.nq" )
		convertCSVPersonsToRDF( persons_csv_path, output_file_persons )

	elif from_source == "table":
		YAML_MAIN = config_local.get( "YAML_MAIN" )
		main_dir = os.path.dirname( YAML_MAIN )
		sys.path.insert( 0, main_dir )
		from hsn_links_db import Database, format_secs, get_archive_name, none2empty
		
		config_main = get_yaml_config( YAML_MAIN )
		
		HOST_LINKS   = config_main.get( "HOST_LINKS" )
		USER_LINKS   = config_main.get( "USER_LINKS" )
		PASSWD_LINKS = config_main.get( "PASSWD_LINKS" )
		DB_NAME      = "links_cleaned"
		
		print( "HOST_LINKS: %s" % HOST_LINKS )
		print( "USER_LINKS: %s" % USER_LINKS )
		print( "PASSWD_LINKS: %s" % PASSWD_LINKS )
		
		db = Database( host = HOST_LINKS, user = USER_LINKS, passwd = PASSWD_LINKS, dbname = DB_NAME )
		
		config_params = {
			"match_function"         : config_local.get( "MATCH_FUNCTION" ),
			"id_sources"             : config_local.get( "ID_SOURCES" ),
			"source_name"            : config_local.get( "SOURCE_NAME", "" ),
			"registration_fields"    : config_local.get( "REGISTRATION_FIELDS" ),
			"person_fields_common"   : config_local.get( "PERSON_FIELDS_COMMON" ),
			"person_fields_birth"    : config_local.get( "PERSON_FIELDS_BIRTH" ),
			"person_fields_marriage" : config_local.get( "PERSON_FIELDS_MARRIAGE" ),
			"person_fields_death"    : config_local.get( "PERSON_FIELDS_DEATH" ),
			"rdf_merge_samples"      : config_local.get( "RDF_MERGE_SAMPLES", "" )
		}
		
		print( "match_function:         %s" % config_params[ "match_function" ] )
		print( "id_sources:             %s" % config_params[ "id_sources" ] )
		print( "source_name:            %s" % config_params[ "source_name" ] )
		print( "registration_fields:    %s" % config_params[ "registration_fields" ] )
		print( "person_fields_common:   %s" % config_params[ "person_fields_common" ] )
		print( "person_fields_birth:    %s" % config_params[ "person_fields_birth" ] )
		print( "person_fields_marriage: %s" % config_params[ "person_fields_marriage" ] )
		print( "person_fields_death:    %s" % config_params[ "person_fields_death" ] )
		print( "rdf_merge_samples:      %s" % config_params[ "rdf_merge_samples" ] )
		
		match_function = config_params[ "match_function" ]
		
		if match_function:
			matches_params = get_matches_params()
			match_params   = matches_params.get( match_function )
			
			make_rdf_by_function( rdf_dir, match_params, config_params )
		else:
			make_rdf_cbg( db, rdf_dir, config_params )

	msg = "Stop: %s" % datetime.datetime.now()
	
	str_elapsed = format_secs( time() - time0 )
	print( "Creating RDF took %s" % str_elapsed )

# [eof]
