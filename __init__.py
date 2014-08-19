"""Simple no-SQL database implemented using Python SQLite and designed for Flask web applications.

A database within a database, and there's no sequel.

Initialise the database by creating a new instance, specifying the path. If this isn't an existing database, use the __dbinit()
method to initialise (create the tables etc.):

	>>> db = Database('__test__.db')
	>>> db._Database__dbinit()

Save dictionaries (containing nested lists/dictionaries as needed) for future retrieval:

	>>> db.save(dict(list=[1, 2, 3], name='One'), collection='test')
	>>> db.save(dict(list=[2, 3, 4], name='Two'), collection='test')
	>>> db.save(dict(list=[3, 4, 5], name='Three'), collection='test')
	>>> db.get(collection='test')
	[{'_collection': u'test', '_id': 1, u'list': [1, 2, 3], u'name': u'One'}, {'_collection': u'test', '_id': 2, u'list': [2, 3, 4], u'name': u'Two'}, {'_collection': u'test', '_id': 3, u'list': [3, 4, 5], u'name': u'Three'}]

You can query results by providing a query dictionary to the .get(..) method:

	>>> db.get('test', {'name': 'One'})
	[{'_collection': u'test', '_id': 1, u'list': [1, 2, 3], u'name': u'One'}]

Alternatively, some of the methods provided can be functions, which will be used to test the values:

	>>> db.get('test', {'list': lambda lst: (4 in lst)})
	[{'_collection': u'test', '_id': 2, u'list': [2, 3, 4], u'name': u'Two'}, {'_collection': u'test', '_id': 3, u'list': [3, 4, 5], u'name': u'Three'}]

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
	[{u'name': u'Two', u'_collection': u'test', u'x': 5, '_id': 2, u'list': [2, 3, 4]}, {u'name': u'Three', u'_collection': u'test', u'x': 5, '_id': 3, u'list': [3, 4, 5]}]

Delete items directly with their id:

	>>> db.delete_by_id(2)

Or by using the objects returned from get:

	>>> db.delete(db.get('test', {'name': 'Three'})[0])

Which will have removed them from the database:

	>>> db.get('test')
	[{u'name': u'One', u'_collection': u'test', '_id': 1, u'list': [1, 2, 3], u'hello': u'world'}]

Like sqlite3, the database is in a file in the filesystem which can be copied/deleted/etc: 

	>>> import os; os.remove('__test__.db')
"""

import sqlite3

try:
	import simplejson as json
except ImportError:
	import json

def inception_factory(cursor, row):
	rv = json.loads(row[2])
	rv.update(_id=row[0],
			  _collection=unicode(row[1]))
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

class Database():

	def __init__(self, path, app=None):
		self.dbpath = path
		self.app = app
		if app:
			from flask import g
			self.__dbclose = app.teardown_appcontext(self.__dbclose)

	def __dbconnect(self):
		rv = sqlite3.connect(self.dbpath, detect_types=sqlite3.PARSE_DECLTYPES)
		rv.row_factory = inception_factory
		return rv

	def __dbget(self):
		if self.app:
			if not hasattr(g, 'inception_sqlite_db'):
				g.inception_sqlite_db = self.__dbconnect()
			return g.inception_sqlite_db
		else:
			return self.__dbconnect()

	def __dbclose(self):
		if self.app:
			if hasattr(g, 'inception_sqlite_db'):
				g.inception_sqlite_db.close()

	def __dbinit(self):
		db = self.__dbget()
		db.execute('drop table if exists inception;')
		db.execute('create table inception (id integer primary key autoincrement, collection text not null, document text);')
		db.commit()

	def get_by_id(self, id):
		db = self.__dbget()
		return db.execute('select * from inception where id = ?', (id,)).fetchone()

	def get(self, collection=None, query=None):
		db = self.__dbget()
		
		if collection:
			sql, params = 'select * from inception where collection = ?', (collection,)
		else:
			sql, params = 'select * from inception', ()

		results = db.execute(sql, params).fetchall()
		if query:
			results = filter_results(results, query)

		return results

	def save(self, document, collection=None):
		collection = document.get('_collection', None) or collection
		if not collection:
			collection = ''

		docid = document.pop('_id', None)
		if docid:
			sql, params = ('insert or replace into inception (id, collection, document) values (?, ?, ?)',
						   (int(docid), collection, json.dumps(document)))
		else:
			sql, params = ('insert or replace into inception (collection, document) values (?, ?)',
						   (collection, json.dumps(document)))

		db = self.__dbget()
		db.execute(sql, params)
		db.commit()

	def save_all(self, documents, collection=None):
		db = self.__dbget()
		
		for document in documents:
			collection = document.get('_collection', None) or collection
			if not collection:
				collection = ''

			docid = document.pop('_id', None)
			if docid:
				sql, params = ('insert or replace into inception (id, collection, document) values (?, ?, ?)',
						   (docid, collection, json.dumps(document)))
			else:
				sql, params = ('insert or replace into inception (collection, document) values (?, ?)',
							   (collection, json.dumps(document)))
			db.execute(sql, params)

		db.commit()

	def delete_by_id(self, id):
		db = self.__dbget()
		db.execute('delete from inception where id = ?', (id,))
		db.commit()

	def delete(self, document):
		if '_id' in document.keys():
			self.delete_by_id(document['_id'])

def _test():
	import doctest
	doctest.testmod()

if __name__ == '__main__':
	_test()