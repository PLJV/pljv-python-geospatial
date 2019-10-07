#!/usr/bin/env python3

"""
Classes and modules implemented below are essentially wrappers
around GDAL/Numpy primatives. Some higher-level hooks for GeoRasters 
and Google Earth Engine are provided that allow easy access to 
raster manipulations from these interfaces. The goal here is 
not to re-invent the wheel. It's to lean-on the base 
functionality of other frameworks where we can and use GDAL
and NumPy as a base for extending the functionality of GeoRasters 
et al only where needed.
"""

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2019, Playa Lakes Joint Venture"
__credits__ = ["Kyle Taylor", "Alex Daniels", "Meghan Bogaerts","Stephen Chang"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

# logging
import logging

logger = logging.getLogger(__name__)

# mmap file caching and file handling
import sys, os
import re
from random import randint
from copy import copy
# raster manipulation
from georasters import GeoRaster, get_geo_info, create_geotiff, merge
import gdalnumeric
import gdal
import numpy as np
from osgeo import gdal_array
# memory profiling
import types
import psutil
# beatbox
from .network import PostGis

_DEFAULT_NA_VALUE = 65535
_DEFAULT_DTYPE = np.uint16
_DEFAULT_RASTER_FORMAT = gdal.GetDriverByName('GTiff')

# short-hand string identifiers for numpy
# types. Int, float, and byte will be the
# most relevant for raster arrays, but the
# gang is all here

_NUMPY_TYPES = {
  "int": np.intc,
  "uint8": np.uint8,
  "int8": np.uint8,
  "byte": np.int8,
  "int16": np.int16,
  "int32": np.int32,
  "uint16": np.uint16,
  "uint32": np.uint32,
  "float": np.single,
  "float32": np.float32,
  "float64": np.float64,
  "complex64": np.complex64,
  "complex128": np.complex128
}

def _split(raster=None, n=None):
    """
    Wrapper for np._array_split. Splits an input array into n (mostly)
    equal segments, possibly for a parallelized operation.
    :param np.array raster:
    :param int n: number of splits to use for np.array_split 
    :return:
    """
    # args[0]/raster=
    if raster is None:
        raise IndexError("invalid raster= argument specified")
    #args[1]/n=
    if n is None:
        raise IndexError("invalid n= argument specified")
    return np.array_split(
        np.array(raster.array, dtype=str(raster.array.data.dtype)),
        n
    )


def _ram_sanity_check(array=None):
    """
    Determine if there is enough ram for an operation and return 
    the amount of ram available (in bytes) as a dictionary 
    :param np.array array: NumPy array 
    :return dict:
    """
    if array is None:
        raise IndexError("first pos. argument should be some kind of "
                         "raster data")

    _cost = _est_free_ram() - _est_array_size(array)
    return {
        'available': bool(_cost > 0),
        'bytes': int(_cost)
    }

def _no_data_value_sanity_check(obj=None):
    """ Checks a Raster object for a sane no data value. Occasionally a
    user-supplied raster file will contain a type-mismatch between the
    raster's no data value (e.g., a value less-than 0) and it's
    stated data type (unsigned integer). Returns a sane no data value
    and a warning, or the original value if is good to use.
    """
    if str(obj.dtype).find('u') is not -1: # are we unsigned?
        if obj.ndv < 0:
            logger.warning("no data value for raster object is less-than 0,"
                "but our data type is unsigned. Forcing a no data value of 0.")
            return(0)
    return(obj.ndv)

def _est_free_ram():
    """ Determines the amount of free ram available for an operation. 
    :return: int (free ram measured in bytes)
    """
    return psutil.virtual_memory().free


def _est_array_size(obj=None, byte_size=None, dtype=None):
    """ Estimate the total size (in bytes) an array-like object will consume
    :param args:
    :return:
    """
    # args[0] is a list containing array dimensions
    if isinstance(obj, list) or isinstance(obj, tuple):
        _array_len = np.prod(obj)
    elif isinstance(obj, GeoRaster):
        dtype = obj.datatype
        _array_len = np.prod(obj.shape)
        _byte_size = _to_numpy_type(obj.datatype)
    elif isinstance(obj, Raster):
        dtype = obj.array.dtype
        _array_len = np.prod(obj.array.shape)
        _byte_size = _to_numpy_type(obj.array.dtype)
    else:
        _array_len = len(obj)

    if dtype is not None:
        _byte_size = sys.getsizeof(_to_numpy_type(dtype))
    else:
        raise IndexError("couldn't assign a default data type and an invalid"
                         " dtype= argument specified")
    return _array_len * _byte_size


def _process_blockwise(*args):
    """
    Accepts an array object and splits it into chunks that can be handled
    stepwise
    :param np.array np.array: NumPy array object
    :return:
    """
    _array = args[0].raster   # numpy array
    _rows = _array.shape[0]   # rows in array
    _n_chunks = 1             # how many blocks (rows) per chunk?
    """Yield successive n-sized chunks from 0-to-nrow."""
    for i in range(0, _rows, _n_chunks):
        yield _array[i:i + _n_chunks]


def _is_raster(obj=None):
    try:
        if isinstance(obj, Raster):
            return True
        else:
            return False
    except:
        return False

def _is_array(obj=None):
    try:
        if isinstance(obj, np.ma.core.MaskedArray):
            return True
        else:
            return False
    except Exception:
        return False

def _is_number(num_list=None):
    """
    Determine whether any item in a list is not a number.
    :param args[0]: a python list object
    :return: True on all integers,
    """
    try:
        if np.sum([not(isinstance(i, int) or isinstance(i, float))
                   for i in num_list]) > 0:
            return False
        else:
            return True
    except ValueError:
        return False


def _is_wkt_str(wkt=None, *args):
    """
    Returns a boolean if a user-provided string can be parsed by GDAL as WKT

    :param str wkt: A GDAL-formatted WKT string; e.g., that is can be used to open rasters on a SQL server.
    :return: Returns true if the wkt str appear valid
    :rtype: Boolean
    """
    raise NotImplementedError

def _is_valid_path(path):
    _is_valid = False
    try:
        if os.path.exists(path):
            _is_valid = True
    except TypeError:
        _is_valid = False
    except ValueError:
        _is_valid = False

    return _is_valid

def slope(array):
    x, y = np.gradient(array)
    return(np.pi/2. - np.arctan(np.sqrt(x*x + y*y)))

def aspect(array):
    x, y = np.gradient(array)
    return np.arctan2(-x, y)

class NdArrayDiscCache(object):
    def __init__(self, **kwargs):
        """
        ArrayDiscCache helps build a local numpy memmap file and either loads raster data from disc into the cache, 
        or attempts a direct assignment of a user-specific numpy array to a cache file (via the input= argument).
        :param str input: Either a full path to a raster file memmap'd to disk or an nd array object that is mapped directly
        :param str dtype: Numpy datatype specification to use for our array cache
        :param str x_size: Number of cells (x-axis) to use for our array cache
        :param str y_size: Number of cells (y-axis) to use for our array cache
        """
        self.disc_cache_file = kwargs.get('cache_filename', str(randint(1, 9E09)) + '_np_array.dat')
        self.dtype = kwargs.get('dtype', _DEFAULT_DTYPE)
        self.x_size = kwargs.get('x_size')
        self.y_size = kwargs.get('y_size')
        self.ndv = kwargs.get('ndv', _DEFAULT_NA_VALUE)

        logger.debug("Using disc caching file for large numpy array: " + self.disc_cache_file)

        # if the user specified a valid file path or numpy array object, try to read it into our cache
        # but otherwise allow an empty specification
        if kwargs.get('input') is None:
            # by default, just create an empty cache file from the parameters specified by the user
            self.array = np.memmap(
                filename=self.disc_cache_file, dtype=_to_numpy_type(self.dtype), mode='w+',
                shape = (self.y_size, self.x_size)
            )
        else:

            if _is_valid_path(kwargs.get('input')):
                logger.debug("Loading file contents into disc cache file:" + self.disc_cache_file)
                _raster_file = gdal.Open(kwargs.get('input'))
                self.array = np.memmap(
                    filename=self.disc_cache_file, dtype=_to_numpy_type(self.dtype), mode='w+',
                    shape = (_raster_file.RasterYSize, _raster_file.RasterXSize)
                )
                del _raster_file 
                self.array[:] = gdalnumeric.LoadFile(
                    filename=kwargs.get('input'),
                    buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(_to_numpy_type(self.dtype))
                )[:]
            else:
                logger.debug("Treating input= object as numpy array object and reading into disc cache file:" + self.disc_cache_file)
                self.array = np.memmap(
                    filename=self.disc_cache_file, dtype=kwargs.get('input').dtype, mode='w+',
                    shape = kwargs.get('input').shape
                )
                self.array[:] = kwargs.get('input')[:]



class Gdal(object):
    def __init__(self, **kwargs):
        """
        Wrapper for gdal primatives to fetch raster data from a file 
        or SQL database through WKT strings

        :param str file: Full path to a raster file you'd like to open.
        :param str wkt: A GDAL-formatted WKT string; e.g., that can be used to open rasters on a SQL server.
        :param bool use_disc_caching: Should we attempt to read our raster into RAM or should we cache it to disc? 
        """
        self.filename = kwargs.get('file')
        self.wkt = kwargs.get('wkt')
        self.dtype = kwargs.get('dtype',_DEFAULT_DTYPE)
        self.ndv = kwargs.get('ndv', _DEFAULT_NA_VALUE)
        self.x_size = kwargs.get('x_size')
        self.y_size = kwargs.get('y_size')
        self.geot = kwargs.get('geot')
        self.projection = kwargs.get('projection')
        self._use_disc_caching = kwargs.get('use_disc_caching')

        # allow for an empty specification
        if self.filename is not None:
            self.open()
        elif self.wkt is not None:
            logger.debug("WKT string input is not supported yet")
            raise NotImplementedError

    def open(self):
        """
        Read a raster file from disc as a formatted numpy array
        :rval none:
        """
        # grab raster meta informationP from GeoRasters
        try:
            self.ndv, self.x_size, self.y_size, self.geot, self.projection, self.dtype = \
                get_geo_info(self.filename)
        except Exception:
            raise AttributeError("problem processing file input -- is this" +\
                " a raster file?")
        # call gdal with explicit type specification
        # that will store in memory or as a disc cache, depending
        # on the state of our _use_disc_caching property
        if self._use_disc_caching is not None:
            # create a cache file and load file contents into the cache
            _cached_file = NdArrayDiscCache(input=self.filename, dtype=self.dtype, x_size=self.x_size, y_size=self.y_size)
            self.array = _cached_file.array
            self._disc_cache_file = _cached_file.disc_cache_file
        # by default, load the whole file into memory
        else:
            self.array = gdalnumeric.LoadFile(
                filename=self.filename,
                buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(_to_numpy_type(self.dtype))
            )
        # make sure we honor our no data value
        self.array = np.ma.masked_array(
            self.array,
            mask=self.array == self.ndv,
            fill_value=self.ndv
        )

    def read_table(self, *args):
        connection = PostGis(args[0]).to_wkt
        raise NotImplementedError

def _to_numpy_type(user_str):
    """
    Parse our NUMPY_STR dictionary using regular expressions
    for our user-specified function string.
    :param str user_str: User-specified data type as string (e.g., 'int8', 'float32')
    :return:
    """
    user_str = str(user_str).lower()
    for valid_type_str in list(_NUMPY_TYPES.keys()):
        # user might pass a key with extra designators
        # (like np.mean, numpy.median) -- let's
        if bool(re.search(string=valid_type_str, pattern=user_str)):
            return _NUMPY_TYPES[valid_type_str]
    # default case
    return None

class Raster(object):
    def __init__(self,**kwargs):
        """
        Raster class will intuit how to read raster data depending on the context of user-input.
        :param input: input string that can be a dictionary of named arguments, JSON, or a full-path to a file to read from disc.
        """
        self.array = []
        self.crs = []
        self.crs_wkt = []
        self.geot = None
        self._use_disc_caching = None
        self._disc_cache_file = None

        # allow for an empty specification by user
        if kwargs.get('input') is not None:
            self._builder(kwargs)

    def __del__(self):
        logger.debug("Attempting removing local numpy disc caching file: " + self._disc_cache_file)
        if _is_valid_path(self._disc_cache_file):
            os.remove(self._disc_cache_file)

    def _builder(self, config):
        if _is_valid_path(config.get('input')):
            _kwargs = { 'file': config.pop('input') }
            _kwargs.update(config)
            _raster = Gdal(**_kwargs)
        # elif _is_wkt_str(config.get('input')):
        #     _raster = Gdal(wkt=config.get('input'))
        elif _is_array(config.get('input')):
            _raster = Raster()
            _raster.array[:] = config.get('input')[:]
        elif _is_raster(config.get('input')):
            _raster = config.get('input')
            if _raster._use_disc_caching:
                self.array = NdArrayDiscCache(
                    input=_raster.array, 
                    dtype=_raster.dtype, 
                    x_size=_raster.x_size, 
                    y_size=_raster.y_size
                )
        else:
            # allow an empty specification
            _raster = Raster()
            self.array = _raster.array

        self.geot = _raster.geot
        self.ndv = _raster.ndv
        self.projection = _raster.projection
        self.dtype = _raster.dtype
        self._use_disc_caching = _raster._use_disc_caching
        self._disc_cache_file = _raster._disc_cache_file

    def to_georaster(self):
        """ Parses internal Raster elements and returns as a clean GeoRaster
        object.
        :return:
        """
        return GeoRaster(
            self.array,
            self.geot,
            nodata_value=self.ndv,
            projection=self.projection,
            datatype=self.dtype
        )

    def to_numpy_array(self):
        """ Returns numpy array values for our Raster object.
        :return:
        """
        return self.array
