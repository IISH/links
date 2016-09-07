#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:		Fons Laan, KNAW IISH - International Institute of Social History
Project:	LINKS
Name:		export_reports.py
Version:	0.1
Goal:		collect error log tables into a single table ERROR_STORE
Notice:		See the variable x_codes below. If the ref_report table is updated 
			with new report_type values for 'x' codes, this variable must also 
			be updated. 

07-Sep-2016 Created
07-Sep-2016 Changed
"""

# python-future for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, map, next, 
	oct, open, pow, range, round, str, super, zip )

import os
import sys
import datetime
from time import time
from dateutil.parser import parse
import MySQLdb

debug = False



# [eof]
