from django.conf import settings
from django.utils.importlib import import_module
from redis_entity import *
_MODULE_NAMES = getattr(settings, 'REDIS_SETTINGS_MODULES', ())
from redis.exceptions import WatchError
import hashlib

#TODO update might overwrite field indexes

INDEX_KEY_INFIX = "redis_index"

SWITH_INDEX_SEPARATOR = '\x00'

isiterable = lambda obj: getattr(obj, '__iter__', False)

import datetime

def val_for_insert(d):
	if isinstance(d,unicode):
	  d = d
	elif isinstance(d,basestring) : d = unicode(d.decode('utf-8'))
	else: d = unicode(d)
        return d


def get_indexes():
        indexes = {}
        for name in _MODULE_NAMES:
            try:
                indexes.update(import_module(name).INDEXES)
            except (ImportError, AttributeError):
                pass
        
	return indexes

def prepare_value_for_index(index,val):
	if index == 'startswith': return val
	if index == 'istartswith': return val.lower()
	if index == 'endswith': return val[::-1]
	if index == 'iendswith': return val.lower()[::-1]
	if index in ('gt','gte','lt','lte'):
		if isinstance(val,datetime.datetime):
			return "%04d%02d%02d%02d%02d" % (val.year,val.month,val.day,val.hour,val.minute)
		if isinstance(val,datetime.time):
			return "%02d%02d" % (val.hour,val.minute)
		if isinstance(val,datetime.date):
			return "%04d%02d%02d" % (val.year,val.month,val.day)
		if isinstance(val,int):
			return "%20d" % val
	return val
	


def create_indexes(column,data,old,indexes,conn,hash_record,table,pk,db_name):
		for index in indexes:
			if index in ('startswith','istartswith','endswith','iendswith','gt','gte','lt','lte'):
				if old is not None:
					if not isiterable(old):
						old = (old,)	
					for d in old:
						d = prepare_value_for_index(index,d)
						conn.zrem(get_zset_index_key(db_name,table,INDEX_KEY_INFIX,column,index),
										d+SWITH_INDEX_SEPARATOR+str(pk))
				if not isiterable(data):
					data = (data,)
				for d in data:
					d = val_for_insert(d)
					d = prepare_value_for_index(index,d)						
					conn.zadd(get_zset_index_key(db_name,table,INDEX_KEY_INFIX,column,index),
										d+SWITH_INDEX_SEPARATOR+str(pk),0)
			if index == 'exact':
				if old is not None:
					if not isiterable(old):
						old = (old,)	
					for d in old:
						conn.srem(get_set_key(db_name,table,column,d),str(pk))
				if not isiterable(data):
					data = (data,)
				for d in data:
					d = val_for_insert(d)
					conn.sadd(get_set_key(db_name,table,column,d),pk)


def delete_indexes(column,data,indexes,conn,hash_record,table,pk,db_name):
		for index in indexes:
			if index in ('startswith','istartswith','endswith','iendswith','gt','gte','lt','lte'):
				if not isiterable(data):
					data = (data,)
				for d in data:
					d = val_for_insert(d)
					d = prepare_value_for_index(index,d)
					conn.zrem(get_zset_index_key(db_name,table,INDEX_KEY_INFIX,column,index),
									d+SWITH_INDEX_SEPARATOR+str(pk))
			if index == 'exact':
				if not isiterable(data):
					data = (data,)
				for d in data:
					d = val_for_insert(d)
					conn.srem(get_set_key(db_name,table,column,d),str(pk))

def filter_with_index(lookup,value,conn,table,column,db_name):
	if lookup in ('startswith','istartswith','endswith','iendswith',):
		value = val_for_insert(value)
		v = prepare_value_for_index(lookup,value)
		
		#v2 = v[:-1]+chr(ord(v[-1])+1) #last letter=next(last letter)
		key = get_zset_index_key(db_name,table,INDEX_KEY_INFIX,column,lookup)

		conn.zadd(key,v,0)
		while True:
			try:
				conn.watch(key)
				up = conn.zrank(key,v)
				r = self._collection.zrange(key,up+1,-1)
				self._collection.zrem(key,v)
		
				ret = set()
				for i in r:
					i = unicode(i.decode('utf8'))
					if i.startswith(v):
						splitted_string = i.split(SWITH_INDEX_SEPARATOR)
						if len(splitted_string) > 1:
							ret.add(splitted_string[-1])
					else:
						break
				return ret
			except WatchError:
				pass

	elif lookup in ('gt','gte','lt','lte'):
		value = val_for_insert(value)
		v = prepare_value_for_index(lookup,value)
		key = get_zset_index_key(db_name,table,INDEX_KEY_INFIX,column,lookup)
		conn.zadd(key,v,0)
		while True:
			try:
				conn.watch(key)
				up = conn.zrank(key,v)
				if lookup in ('lt','lte'):
					r = self._collection.zrange(key,0,up+1)
				else:
					r = self._collection.zrange(key,up+1,-1)
				self._collection.zrem(key,v)
				ret = set()
				for i in r:
					i = unicode(i.decode('utf8'))
					splitted_string = i.split(SWITH_INDEX_SEPARATOR)
					if len(splitted_string) > 0 and\
						 (lookup in ('gte','lte') or\
							"".join(splitted_string[:-1]) != value):
							ret.add(splitted_string[-1])
				return ret
			except WatchError:
				pass
	else:
		raise Exception('Lookup type not supported') #TODO check at index creation?
	


# 
