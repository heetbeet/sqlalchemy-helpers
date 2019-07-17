import sqlalchemy
import sqlalchemy as sq
import re

from sqlalchemy.ext import declarative
Base =  declarative.declarative_base()

import numpy as np

import datetime
import decimal

_type_py2sql_dict = {
    int: sqlalchemy.sql.sqltypes.BigInteger,
    str: sqlalchemy.sql.sqltypes.Unicode,
    float: sqlalchemy.sql.sqltypes.Float,
    decimal.Decimal: sqlalchemy.sql.sqltypes.Numeric,
    datetime.datetime: sqlalchemy.sql.sqltypes.DateTime,
    bytes: sqlalchemy.sql.sqltypes.LargeBinary,
    bool: sqlalchemy.sql.sqltypes.Boolean,
    datetime.date: sqlalchemy.sql.sqltypes.Date,
    datetime.time: sqlalchemy.sql.sqltypes.Time,
    datetime.timedelta: sqlalchemy.sql.sqltypes.Interval,
    list: sqlalchemy.sql.sqltypes.ARRAY,
    dict: sqlalchemy.sql.sqltypes.JSON
}

def type_py2sql(pytype):
    '''Return the closest sql type for a given python type'''
    if pytype in _type_py2sql_dict:
        return _type_py2sql_dict[pytype]
    else:
        raise NotImplementedError(
            "You may add custom `sqltype` to `"+str(pytype)+"` assignment in `_type_py2sql_dict`.")

def type_np2py(dtype=None, arr=None):
    '''Return the closest python type for a given numpy dtype'''
    
    if ((dtype is None and arr is None) or
        (dtype is not None and arr is not None)):
        raise ValueError(
            "Provide either keyword argument `dtype` or `arr`: a numpy array or a numpy dtype.")

    if dtype is None:
        dtype = arr.dtype
        
    #1) Make a single-entry numpy array of the same dtype
    #2) force the array into a python 'object' dtype
    #3) the array entry should now be the closest python type
    single_entry = np.empty([1], dtype=dtype).astype(object)
    
    return type(single_entry[0])

def type_np2sql(dtype=None, arr=None):
    '''Return the closest sql type for a given numpy dtype'''
    return type_py2sql(type_np2py(dtype=dtype, arr=arr))

def type_df2py(df, undefined_to_str=True):
    '''Return a dictionary of the closest python types for the dataframe column types'''
    df_types = {}

    for key in df:

        keytype = type_np2py(df[key].to_numpy().dtype)

        if issubclass(keytype, type(None)):
            if undefined_to_str:
                keytype=str
            else:
                keytype=object

        df_types[key] = keytype
        
    return df_types

def varnameify(s):
    """Slugify a string to a valid python variable name."""
    
    #replace space/dash characters
    s = s.replace(' ','_')
    s = s.replace('-','_')
    
    #remove non- alphabetic/numerical/undercore characters
    s = re.sub('[^0-9a-zA-Z_]', '', s)
    
    #may not start with number
    if s[0] not in ('abcdefghijklmnopqrstuvwxyz'
                    'ABCDEFGHIJKLMNOPQRSTUVWXYZ_'):
        s = '_'+s
    
    return s

def make_table_class(tablename, column_dict, primary_key=None):
    """A class factory to make sqlalchemy tables via declarative_base."""
    
    if primary_key is not None:
        assert primary_key in column_dict, f"Key {primary_key} not found for primary key."
    
    class columns: pass
    for key, val in column_dict.items():
        setattr(columns, key, sq.Column(type_py2sql(val), primary_key=(key==primary_key)))
        
    class table(Base, columns):
        __tablename__ = tablename
        def __repr__(self):
            return str({key:val for key, val in self.__dict__.items() if not key[0] == '_'})
        
    table.__name__ = varnameify(tablename+'Class')
    
    return table