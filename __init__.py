"""Simple no-SQL database designed for Flask web applications. Can use either sqlite3 or mySQL as the base database.

A database within a database, and there's no sequel.

Initialise the database by creating a new instance, specifying the path. If this isn't an existing database, use the _dbinit()
method to initialise (create the tables etc.):

	>>> db = Database('__test__.db')
	>>> db._dbinit()

Save dictionaries (containing nested lists/dictionaries as needed) for future retrieval:

	>>> db.save(dict(list=[1, 2, 3], name='One'), collection='test')
	>>> db.save(dict(list=[2, 3, 4], name='Two'), collection='test')
	>>> db.save(dict(list=[3, 4, 5], name='Three'), collection='test')
	>>> db.get(collection='test')
	[{u'_collection': u'test', u'_id': 1, u'list': [1, 2, 3], u'name': u'One'}, {u'_collection': u'test', u'_id': 2, u'list': [2, 3, 4], u'name': u'Two'}, {u'_collection': u'test', u'_id': 3, u'list': [3, 4, 5], u'name': u'Three'}]

You can query results by providing a query dictionary to the .get(..) method:

	>>> db.get('test', {'name': 'One'})
	[{u'_collection': u'test', u'_id': 1, u'list': [1, 2, 3], u'name': u'One'}]

Alternatively, some of the methods provided can be functions, which will be used to test the values:

	>>> db.get('test', {'list': lambda lst: (4 in lst)})
	[{u'_collection': u'test', u'_id': 2, u'list': [2, 3, 4], u'name': u'Two'}, {u'_collection': u'test', u'_id': 3, u'list': [3, 4, 5], u'name': u'Three'}]

Having retrieved an object, you can edit it, and then save it again using the save() method:

	>>> one = db.get('test', {'name': 'One'})[0]
	>>> one['hello'] = 'world'
	>>> db.save(one)
	>>> db.get('test', {'name': 'One'})[0]['hello']
	u'world'

Or save multiple items at once using save_all():

	>>> edits = db.get('test', {'list': lambda lst: (4 in lst)})
	>>> for item in edits: item['x'] = 5
	>>> db.save_all(edits)
	>>> db.get('test', {'x': 5})
	[{u'_collection': u'test', u'x': 5, u'_id': 2, u'list': [2, 3, 4], u'name': u'Two'}, {u'_collection': u'test', u'x': 5, u'_id': 3, u'list': [3, 4, 5], u'name': u'Three'}]

Delete items directly with their id:

	>>> db.delete_by_id(2)

Or by using the objects returned from get:

	>>> db.delete(db.get('test', {'name': 'Three'})[0])

Which will have removed them from the database:

	>>> db.get('test')
	[{u'_collection': u'test', u'_id': 1, u'list': [1, 2, 3], u'name': u'One', u'hello': u'world'}]

Like sqlite3, the database is in a file in the filesystem which can be copied/deleted/etc:

	>>> import os; os.remove('__test__.db')
"""

import datetime
import sqlite3
import MySQLdb

try:
	import simplejson as json
except ImportError:
	import json

def inception_factory(cursor, row):
	rv = json.loads(row[2])
	rv[u'_id']=row[0]
	rv[u'_collection']=unicode(row[1])
	return rv

def filter_results(results, filters):
	def filter_result(result, filters):
		for (field, flter) in filters.iteritems():
			if callable(flter):
				if not flter(result.get(field, '')):
					return False
			else:
				if not (result.get(field, '') == flter):
					return False
		return True
	return [result for result in results if filter_result(result, filters)]

def inception_serialise(obj):
	if isinstance(obj, datetime.datetime):
		return obj.isoformat()

def _contains(searchtext):
	"""
	Helper functin for testing whether a certain property contains a certain value, avoiding writing one's own lambdas in the
	query.
	"""
	def contains(text):
		return (searchtext in text)

	return contains

class Database(object):

	SQL_DROPTABLE = 'drop table if exists inception;'
	SQL_CREATETABLE = 'create table inception (id integer primary key autoincrement, collection text not null, document text);'
	SQL_SELECT_BY_ID = 'select * from inception where id = ?'
	SQL_SELECT_COLLECTION = 'select * from inception where collection = ?'
	SQL_SELECT_ALL = 'select * from inception'
	SQL_INSERT_WITH_ID = 'insert or replace into inception (id, collection, document) values (?, ?, ?)'
	SQL_INSERT_WITHOUT_ID = 'insert or replace into inception (collection, document) values (?, ?)'
	SQL_DELETE_BY_ID = 'delete from inception where id = ?'

	def __init__(self, path, app=None):
		self.dbpath = path
		self.app = app
		if app:
			from flask import g
			self._dbclose = app.teardown_appcontext(self._dbclose)

	def _dbconnect(self):
		rv = sqlite3.connect(self.dbpath, detect_types=sqlite3.PARSE_DECLTYPES)
		rv.row_factory = inception_factory
		return rv

	def _dbget(self):
		if self.app:
			if not hasattr(g, 'inception__db'):
				g.inception_db = self._dbconnect()
			return g.inception_db
		else:
			return self._dbconnect()

	def _dbclose(self):
		if self.app:
			if hasattr(g, 'inception__db'):
				g.inception_db.close()

	def _dbinit(self):
		db = self._dbget()
		c = db.cursor()
		c.execute(self.SQL_DROPTABLE)
		c.execute(self.SQL_CREATETABLE)
		db.commit()
		c.close()

	def get_by_id(self, id):
		db = self._dbget()
		c = db.cursor()
		c.execute(self.SQL_SELECT_BY_ID, (id,))
		rv = c.fetchone()
		c.close()
		return rv

	def get(self, collection=None, query=None):
		db = self._dbget()

		if collection:
			sql, params = self.SQL_SELECT_COLLECTION, (collection,)
		else:
			sql, params = self.SQL_SELECT_ALL, ()

		c = db.cursor()
		c.execute(sql, params)
		results = c.fetchall()

		if query:
			results = filter_results(results, query)
		c.close()

		return results

	def save(self, document, collection=None):
		collection = document.get('_collection', None) or collection
		if not collection:
			collection = ''

		docid = document.get('_id', None)
		if docid:
			sql, params = (self.SQL_INSERT_WITH_ID,
						   (int(docid), collection, json.dumps(document, default=inception_serialise)))
		else:
			sql, params = (self.SQL_INSERT_WITHOUT_ID,
						   (collection, json.dumps(document, default=inception_serialise)))

		db = self._dbget()
		c = db.cursor()
		c.execute(sql, params)
		db.commit()
		c.close()

	def save_all(self, documents, collection=None):
		db = self._dbget()
		c = db.cursor()

		for document in documents:
			collection = document.get('_collection', None) or collection
			if not collection:
				collection = ''

			docid = document.get('_id', None)
			if docid:
				sql, params = (self.SQL_INSERT_WITH_ID,
						   (docid, collection, json.dumps(document, default=inception_serialise)))
			else:
				sql, params = (self.SQL_INSERT_WITHOUT_ID,
							   (collection, json.dumps(document, default=inception_serialise)))

			c.execute(sql, params)
		
		db.commit()
		c.close()

	def delete_by_id(self, id):
		db = self._dbget()
		c = db.cursor()
		c.execute(self.SQL_DELETE_BY_ID, (id,))
		db.commit()
		c.close()

	def delete(self, document):
		if '_id' in document.keys():
			self.delete_by_id(document['_id'])

class MySQLDatabase(Database):

	SQL_CREATETABLE = 'create table inception (id integer primary key auto_increment, collection text not null, document text);'
	SQL_SELECT_BY_ID = 'select * from inception where id = %s'
	SQL_SELECT_COLLECTION = 'select * from inception where collection = %s'
	SQL_SELECT_ALL = 'select * from inception'
	SQL_INSERT_WITH_ID = 'replace into inception (id, collection, document) values (%s, %s, %s)'
	SQL_INSERT_WITHOUT_ID = 'replace into inception (collection, document) values (%s, %s)'
	SQL_DELETE_BY_ID = 'delete from inception where id = %s'	

	def __init__(self, hostaddress, dbname, username, password, app=None):
		self.hostaddress = hostaddress
		self.dbname = dbname
		self.username = username
		self.password = password
		self.app = app
		if app:
			from flask import g
			self._dbclose = app.teardown_appcontext(self._dbclose)

	def _dbconnect(self):
		return MySQLdb.connect(host=self.hostaddress, user=self.username, passwd=self.password, db=self.dbname)

	def get_by_id(self, id):
		return [inception_factory(None, row) for row in super(MySQLDatabase, self).get_by_id(id)]

	def get(self, collection=None, query=None):
		return [inception_factory(None, row) for row in super(MySQLDatabase, self).get(collection, query)]


def _test():
	import doctest
	doctest.testmod()

if __name__ == '__main__':
	_test()