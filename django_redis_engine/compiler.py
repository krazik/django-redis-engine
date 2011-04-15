import sys
import re
import datetime

from functools import wraps

from django.db.utils import DatabaseError
from django.db.models.fields import NOT_PROVIDED
from django.db.models import F

from django.db.models.sql import aggregates as sqlaggregates
from django.db.models.sql.constants import MULTI, SINGLE
from django.db.models.sql.where import AND, OR
from django.utils.tree import Node
from redis_entity import RedisEntity,split_db_type

from index_utils import get_indexes,create_indexes,delete_indexes,filter_with_index,isiterable,hash_for_redis


import redis


from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler



#TODO pipeline!!!!!!!!!!!!!!!!!!!!!

def safe_regex(regex, *re_args, **re_kwargs):
    def wrapper(value):
        return re.compile(regex % re.escape(value), *re_args, **re_kwargs)
    wrapper.__name__ = 'safe_regex (%r)' % regex
    return wrapper

OPERATORS_MAP = {
    'exact':        lambda val: val,
#    'iexact':       safe_regex('^%s$', re.IGNORECASE),
#    'startswith':   safe_regex('^%s'),
#    'istartswith':  safe_regex('^%s', re.IGNORECASE),
#    'endswith':     safe_regex('%s$'),
#    'iendswith':    safe_regex('%s$', re.IGNORECASE),
#    'contains':     safe_regex('%s'),
#    'icontains':    safe_regex('%s', re.IGNORECASE),
#    'regex':    lambda val: re.compile(val),
#    'iregex':   lambda val: re.compile(val, re.IGNORECASE),
#    'gt':       lambda val: {'$gt': val},
#    'gte':      lambda val: {'$gte': val},
#    'lt':       lambda val: {'$lt': val},
#    'lte':      lambda val: {'$lte': val},
#    'range':    lambda val: {'$gte': val[0], '$lte': val[1]},
#    'year':     lambda val: {'$gte': val[0], '$lt': val[1]},
#    'isnull':   lambda val: None if val else {'$ne': None},
    'in':       lambda val: {'$in': val},
}

NEGATED_OPERATORS_MAP = {
    'exact':    lambda val: {'$ne': val},
    'gt':       lambda val: {'$lte': val},
    'gte':      lambda val: {'$lt': val},
    'lt':       lambda val: {'$gte': val},
    'lte':      lambda val: {'$gt': val},
    'isnull':   lambda val: {'$ne': None} if val else None,
    'in':       lambda val: val
}


def first(test_func, iterable):
    for item in iterable:
        if test_func(item):
            return item

def safe_call(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception,e:
            raise DatabaseError, DatabaseError(str(e)), sys.exc_info()[2]
    return wrapper


class DBQuery(NonrelQuery):
    # ----------------------------------------------
    # Public API
    # ----------------------------------------------
    def __init__(self, compiler, fields):
        super(DBQuery, self).__init__(compiler, fields)
	#print fields
	#print dir(self.query.get_meta())
        self.db_table = self.query.get_meta().db_table
	self.indexes = get_indexes()
	self.indexes_for_model =  self.indexes.get(self.query.model,{})
	self._collection = self.connection.db_connection

        self._ordering = []
        self.db_query = {}

    # This is needed for debugging
    def __repr__(self):
        return '<DBQuery: %r ORDER %r>' % (self.db_query, self._ordering)

    @property
    def collection(self):
        return self._collection

    def fetch(self, low_mark, high_mark):
        results = self._get_results()
	#print 'here results ',results
        primarykey_column = self.query.get_meta().pk.column
        for e_id in results:
            yield RedisEntity(e_id,self._collection,self.db_table,primarykey_column,self.query.get_meta())

    @safe_call
    def count(self, limit=None): #TODO is this right?
        results = self._get_results()
        if limit is not None:
            results = results[:limit]
        return len(results)

    @safe_call
    def delete(self):

	db_table = self.query.get_meta().db_table

	for res in self._get_results():
		for  field in self.query.get_meta().get_all_field_names():

			try:
				db_type, db_subtype =  split_db_type(self.query.get_meta().get_field_by_name(field)[0].db_type())
				if db_type in ('ListField','SetField'):
					val = self._collection.lrange(db_table+'_'+field+'_'+str(res),0,-1)
				else:
					val = self._collection.hget(db_table+'_'+str(res),field)
			except:
				val = None

			if val is not None:
				if not isinstance(val,list) and not isinstance(val,tuple):
					self._collection.srem(db_table+'_'+field+'_'+hash_for_redis(val),str(res))
					self._collection.hdel(db_table+'_'+str(res),field)

				else:
					for v in val:

						self._collection.srem(db_table+'_'+field+'_'+hash_for_redis(v),str(res))
					del self._collection[db_table+'_'+field+'_'+str(res)]
				if field in self.indexes_for_model:
					delete_indexes(	field,
							val,
							self.indexes_for_model[field],
							self._collection,
							db_table+'_'+str(res),
							db_table,
							res,
							)
		self._collection.srem(db_table+'_ids' ,res)
		


    @safe_call
    def order_by(self, ordering):
        if len(ordering) > 1:
		raise DatabaseError('Only one order is allowed')
        for order in ordering:
            if order.startswith('-'):
                order, direction = order[1:], 'desc'
            else:
                direction = 'asc'
            if order == self.query.get_meta().pk.column:
                order = '_id'
            else:
		raise DatabaseError('You can only order by PK')
            self._ordering.append((order, direction))
        return self

    @safe_call
    def add_filter(self, column, lookup_type, negated, db_type, value):
	"""add filter
		used by default add_filters implementation
	
	"""
	#print "ADD FILTER  --  ",column, lookup_type, negated, db_type, value
	if column == self.query.get_meta().pk.column:
		if lookup_type in ('exact','in'):
			#print "cisiamo"
			#print "db_query?"
			#print self.db_query
			try:
				self.db_query[column][lookup_type]
				raise DatabaseError("You can't apply multiple AND filters " #Double filter on pk
                                        "on the primary key. "
                                        "Did you mean __in=[...]?")

			except KeyError:
				self.db_query.update({column:{lookup_type:value}})
	
	else:
		if lookup_type in ('exact','in'):
			self.db_query.update({column:{lookup_type:value}})
		else:
			if column in self.indexes_for_model and lookup_type  in self.indexes_for_model.get(column):
				self.db_query.update({column:{lookup_type:value}})
			
			else:
				raise DatabaseError('Lookup %s on column %s is not allowed (have you tried redis_indexes? )' % (lookup_type,column))
        

    def _get_results(self):
	"""
	see self.db_query, lookup parameters format: {'column': {lookup:value}}
	
	"""
	#print self.db_query
	pk_column = self.query.get_meta().pk.column
	db_table = self.query.get_meta().db_table

	
	
	
	results = self._collection.smembers(db_table+'_ids')
	#print "GET RES", self.db_query
	#print "RESULTS ORA",results
	for column,filteradd in self.db_query.iteritems():
		lookup,value = filteradd.popitem()#TODO tuple better?
		#print "COSE:",pk_column == column,pk_column, column,value
		if pk_column == column:
			if lookup == 'in': #TODO meglio??
				results = results & set(value)   #IN filter
			elif lookup == 'exact':
				results = results & set([value,])
				
		else:
			if lookup == 'exact':
				results = results & self._collection.smembers(db_table+'_'+column+'_'+hash_for_redis(value))
			elif lookup == 'in': #ListField or empty
				tempset = set()
				for v in value:
					tempset = tempset.union(self._collection.smembers(db_table+'_'+column+'_'+hash_for_redis(v) ) )
				results = results & tempset
			else:
				tempset = filter_with_index(lookup,value,self._collection,db_table,column)
				if tempset is not None:
					results = results & tempset
				else:
					results = set()
								
	#print results
        if self._ordering:
	    if self._ordering[0][1] == 'desc': 
		results.reverse()
	
	if self.query.low_mark > 0 and self.query.high_mark is not None:
		results = list(results)[self.query.low_mark:self.query.high_mark]
        elif self.query.low_mark > 0:

            results = list(results)[self.query.low_mark:]

        elif self.query.high_mark is not None:
            results = list(results)[:self.query.high_mark]
	#print list(results )
        return list(results )

class SQLCompiler(NonrelCompiler):
    """
    A simple query: no joins, no distinct, etc.
    """
    query_class = DBQuery

    def _split_db_type(self, db_type):
        try:
            db_type, db_subtype = db_type.split(':', 1)
        except ValueError:
            db_subtype = None
        return db_type, db_subtype

    @safe_call # see #7
    def convert_value_for_db(self, db_type, value):
	#print db_type,'   ',value
        if db_type is None or value is None:
            return value

        db_type, db_subtype = self._split_db_type(db_type)
        if db_subtype is not None:
            if isinstance(value, (set, list, tuple)):
                
                return [self.convert_value_for_db(db_subtype, subvalue)
                        for subvalue in value]
            elif isinstance(value, dict):
                return dict((key, self.convert_value_for_db(db_subtype, subvalue))
                            for key, subvalue in value.iteritems())

        if isinstance(value, (set, list, tuple)):
            # most likely a list of ObjectIds when doing a .delete() query
            return [self.convert_value_for_db(db_type, val) for val in value]

        if db_type == 'objectid':
            return value
        return value

    @safe_call # see #7
    def convert_value_from_db(self, db_type, value):
        if db_type is None:
            return value

        if value is None or value is NOT_PROVIDED:
            # ^^^ it is *crucial* that this is not written as 'in (None, NOT_PROVIDED)'
            # because that would call value's __eq__ method, which in case value
            # is an instance of serializer.LazyModelInstance does a database query.
            return None

        db_type, db_subtype = self._split_db_type(db_type)
        if db_subtype is not None:
            for field, type_ in [('SetField', set), ('ListField', list)]:
                if db_type == field:
                    return type_(self.convert_value_from_db(db_subtype, subvalue)
                                 for subvalue in value)
            if db_type == 'DictField':
                return dict((key, self.convert_value_from_db(db_subtype, subvalue))
                            for key, subvalue in value.iteritems())

        if db_type == 'objectid':
            return unicode(value)

        if db_type == 'date':
            return datetime.date(value.year, value.month, value.day)

        if db_type == 'time':
            return datetime.time(value.hour, value.minute, value.second,
                                 value.microsecond)
        return value

    def insert_params(self):
        conn = self.connection
        params = {'safe': conn.safe_inserts}
        if conn.wait_for_slaves:
            params['w'] = conn.wait_for_slaves
        return params

    @property
    def _collection(self):
        #TODO multi db
	return self.connection.db_connection

    def _save(self, data, return_id=False):

	db_table = self.query.get_meta().db_table
	indexes = get_indexes()
	indexes_for_model =  indexes.get(self.query.model,{})
	#TODO: multi_db name
	if '_id' in data:
		pk = data['_id']
		new = False
	else:
		pk = self._collection.incr(db_table+"_id")
		new = True
		
	
	for key,value in data.iteritems():
		
		if new:
			
			if not isinstance(value,tuple) and not isinstance(value,list):
				self._collection.hset(db_table+'_'+str(pk),key,value)
				self._collection.sadd(db_table+'_'+key+'_'+hash_for_redis(value),pk)
			else:
				for v in value:
					self._collection.sadd(db_table+'_'+key+'_'+hash_for_redis(v),pk)
					self._collection.lpush(db_table+'_'+key+'_'+str(pk),v)
		else:
			if not isinstance(value,tuple) and not isinstance(value,list):
				old = self._collection.hget(db_table+'_'+str(pk),key)
			else:
				old = self._collection.lrange(db_table+'_'+key+'_'+str(pk),0,-1)
			if old != value:
				
				if not isinstance(value,tuple) and not isinstance(value,list):
					self._collection.hset(db_table+'_'+str(pk),key,value)
					self._collection.srem(db_table+'_'+key+'_'+hash_for_redis(old),pk)
					self._collection.sadd(db_table+'_'+key+'_'+hash_for_redis(value),pk)
				else:

					for v in old:
						if v not in value: self._collection.srem(db_table+'_'+key+'_'+hash_for_redis(v),pk)
					for v in value:
						if v not in old: self._collection.sadd(db_table+'_'+key+'_'+hash_for_redis(v),pk)
						self._collection.lpush(db_table+'_'+key+'_'+str(pk),v)
						


		if key in indexes_for_model:
			create_indexes(	key,
					value,
					indexes_for_model[key],
					self._collection,
					db_table+'_'+str(pk),
					db_table,
					pk,
					)
	
        if '_id' not in data: self.connection.db_connection.sadd(db_table+"_ids" ,pk)

	
        if return_id:
            return unicode(pk)

    def execute_sql(self, result_type=MULTI):
        """
        Handles aggregate/count queries
        """
	
	
	raise NotImplementedError('Not implemented')
	

class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    @safe_call
    def insert(self, data, return_id=False):
        pk_column = self.query.get_meta().pk.column
        try:
            data['_id'] = data.pop(pk_column)
        except KeyError:
            pass
        return self._save(data, return_id)

class SQLUpdateCompiler(NonrelUpdateCompiler, SQLCompiler):
    pass
class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    pass
