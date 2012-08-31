from django.db import models
from django.db.models.fields import FieldDoesNotExist, NOT_PROVIDED
import hashlib
import pickle

class RedisEntity(object):
    def __init__(self, e_id, connection, db_table, pkcolumn, querymeta, db_name, data=None):
        self.id = e_id
        self.connection = connection
        self.db_table = db_table
        self.pkcolumn = pkcolumn
        self.querymeta = querymeta
        self.db_name = db_name
        if not data:
            self.data = self.connection.hgetall(get_hash_key(self.db_name, self.db_table, self.id))
        else:
            self.data = data
		
    def get(self, what, value):
        if what == self.pkcolumn:
            return self.id
        raw_value = self.data.get(what, value)
        # saved below, in theory you chould start with no data and get fields 1 at a time, but not using this
        # raw_value = self.connection.hget(get_hash_key(self.db_name,self.db_table,self.id), what)
            
        rv = None
        try:
            if isinstance(self.querymeta.get_field(what), models.IntegerField):
                # don't pickle integers
                rv = int(raw_value)
            elif isinstance(self.querymeta.get_field(what), models.FloatField):
                rv = float(raw_value)
            elif isinstance(self.querymeta.get_field(what), models.DecimalField):
                rv = Decimal(raw_value)
        except:
            pass

        if not rv:
            # didn't get set, unpickle it
            rv = unpickle(raw_value)

        return rv

def split_db_type(db_type):
    #TODO move somewhere else
    try:
        db_type, db_subtype = db_type.split(':', 1)
    except ValueError:
        db_subtype = None
    return db_type, db_subtype

def get_hash_key(db_name, db_table, pk):
    return db_name+'_'+db_table+'_'+str(pk)

def get_zset_index_key(db_name, db_table, infix, column, index):
    return db_name+'_'+db_table +'_' + infix + '_' + column + '_'+index

def get_list_key(db_name, db_table, key, pk):
    return db_name+'_'+db_table+'_'+key+'_'+str(pk)

def get_set_key(db_name, db_table, key, value):
    return db_name+'_'+db_table+'_'+key+'_'+hash_for_redis(value)

def unpickle(val):
    if val is None:
        return None
    else:
        try:
            return pickle.loads(val)
        except:
            # if we failed to unpickle, just return the raw value
            return val

def enpickle(val):
    if val is None:
        return None
    else:
        return pickle.dumps(val)

def hash_for_redis(val):
    if isinstance(val,unicode):
        return hashlib.md5(val.encode('utf-8')).hexdigest()
    else:
        return hashlib.md5(str(val)).hexdigest()