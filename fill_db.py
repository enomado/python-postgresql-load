
import re
import json
from itertools import count
from shutil import copyfileobj
import urlparse
import os.path
import shutil
import time

import os.path
import sys
import logging
import random 

from struct import pack
from cStringIO import StringIO

import unicodecsv
import requests

import sqlalchemy

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy import *

# http://stackoverflow.com/questions/8144002/use-binary-copy-table-from-with-psycopg2
# http://stackoverflow.com/a/8150329
# https://github.com/pbrumm/pg_data_encoder/blob/master/lib/pg_data_encoder/encode_for_copy.rb
# http://pydoc.net/Python/pg_typical/1.1/postgresql.protocol.typical.stdio/
# http://pydoc.net/Python/pg_typical/1.1/postgresql.protocol.typical.pstruct/

from .main import files_directory

def encode_object(copy_obj):
	tmo_obj = {}
	hstore_obj = {}

	for k, v in copy_obj.viewitems():
		if k in scheme:
			tmo_obj[k] = v
		else:
			hstore_obj[k] = v

	fields = []
	for i in scheme:
		fields.append(tmo_obj.get(i))

	return fields, hstore_obj


class CopyBinaryStream(object):

	def __init__(self, objects_stream, col_len):
		# self.st = StringIO()
		self.co = 0
		self.objects_stream = objects_stream
		self.col_len = col_len
		self.state = 0

	def readline(self, size=None):
		cpy = StringIO()

		if self.state == 0: # start signature
			cpy.write(pack('!11sii', b'PGCOPY\n\377\r\n\0', 0, 0))
			cpy.seek(0)
			t = cpy.getvalue()
			self.state = 1 
			return t
		elif self.state == 2: 			# File trailer
			cpy.write(pack('!h', -1))
			cpy.seek(0)
			t = cpy.getvalue()
			self.state == 3
			return t
		elif self.state == 3:
			return ''

		try:
			fields_all, hs = self.objects_stream.next()
		except StopIteration:
			self.state = 2
			return ''

		cpy.write(pack('!h', col_len))

		for value in fields_all:
			if value is None:
				hh =  pack('!i0s', 0, "")
				cpy.write(hh)
			else:
				value = unicode(value).encode('utf8')
				size = len(value)
				hh =  pack('!i{}s'.format(size), size, value)
				cpy.write(hh)

		# hstore
		hstore = StringIO()
		hh =  pack('!i', len(hs)) # overall
		hstore.write(hh)

		for kk, vv in hs.viewitems():
			k = unicode(kk).encode('utf8')
			hh =  pack('!i{}s'.format(len(k)), len(k), k)
			hstore.write(hh)

			v = unicode(vv).encode('utf8')
			hh =  pack('!i{}s'.format(len(v)), len(v), v)
			hstore.write(hh)

		hstore.seek(0)
		gg = hstore.getvalue()

		hh =  pack('!i', len(gg)) # overall
		cpy.write(hh)
		cpy.write(gg)

		# Copy data to database
		cpy.seek(0)

		t = cpy.getvalue()
		# if hs:
		# 	t = t + "|" + hs
		# import IPython; IPython.embed()
		# if hs:
		# 	print repr(t)
		# print repr(t)
		# self.co += 1
		return t

	read = readline



files_obj = get_dummy_files()
ffiles = list(files_obj)
random.shuffle(ffiles)

col_len = len(columns)

####




def process_file(file_obj):
	for item in split_by_lines(unzipper(file_obj)):
		try:
			i = json.loads(item)
			yield i
		except Exception as ex: 
			print "ex '{}'".format(item)
			raise


def object_stream_from_file(file_obj):
	y = process_file(file_obj)
	for i in y:
		h, hs = encode_object(i)
		# print h
		yield h, hs


def create_if_not_exists(filename):
	# try:
	f = DumpFile()
	f.filename = filename
	f.status = 'new'
	session.add(f)
	try:
		session.commit()
	except sqlalchemy.exc.IntegrityError as ex:
		session.rollback()
		print 'okay'
	# except:


# FOR UPDATE [ OF table_name [, ...] ] [ NOWAIT ]

def process_process_file(fullpath):
	if not os.path.exists(fullpath):
		raise Exception('no file %s', fullpath)

	filename = os.path.basename(fullpath)
	create_if_not_exists(filename)

	try:
		file_obj = DumpFile.query.filter(DumpFile.filename == filename).filter(DumpFile.status == 'new').with_for_update(nowait=True).one()
	except sqlalchemy.exc.OperationalError as ex:
		session.rollback()
		print 'locked'
		return None
	except sqlalchemy.orm.exc.NoResultFound as ex:
		print 'okay'
		return 

	stream = object_stream_from_file(file_obj.open())

	binary_stream = CopyBinaryStream(stream, col_len)

	conn = session.connection()  # SQLAlchemy Connection 
	dbapi_conn = conn.connection  # DBAPI connection (technically a connection pool wrapper called ConnectionFairy, but everything is there) 
	cursor = dbapi_conn.cursor()  # actual DBAPI cursor 

	# try:
	tpl = "COPY stats({}) FROM STDIN WITH BINARY".format(", ".join(columns))
	cursor.copy_expert(tpl, binary_stream)
	# except: 
	# 	import IPython; IPython.embed()

	session.flush()
	file_obj.status = 'done'

	print 'done {}'.format(file_obj.filename, )
	session.commit()


def main():
	for ffile in ffiles:
		try:
			process_process_file(ffile)
		except Exception as ex:
			session.rollback()
			print ex


if __name__ == "__main__":
	main()
	

