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
__credits__ = ["Kyle Taylor", "Alex Daniels", "Meghan Bogaerts",
               "Stephen Chang"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

# logging
import logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# mmap file caching and file handling
import sys
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


def crop(*args):
    return _local_crop(args)

def extract(*args):
    """
    Accept a series of 'with' arguments and uses an appropriate
    backend to perform an extract operation with raster data
    :param args:
    :return:
    """

def binary_reclassify(array=None, match=None, *args):
    """
    Generalized version of binary_reclassify that can accomodate a local numpy 
    array or processing on EE
    :param args:
    :return:
    """
    _backend = 'local'
    # args[0]/array=
    if array is None:
        raise IndexError("invalid raster= argument provided by user")
    # args[1]/match=
    if match is None:
        raise IndexError("invalid match= argument provided by user")
    if not _is_number(match):
        logger.warning("One or more values in your match array are "
                       "not integers -- the reclass operation may produce "
                       "unexpected results")
    # process our array using the appropriate backend,
    # currently only local operations are supported
    if isinstance(array, Raster):
        _backend = 'local'
        array = array.to_georaster()
    elif isinstance(array, GeoRaster):
        _backend = 'local'
    elif isinstance(array, np.array):
        _backend = 'local'
    else:
        _backend = 'unknown'

    if _backend == "local":
        return _local_binary_reclassify(array, match)
    else:
        raise NotImplementedError("Currently only local binary "
                                  "reclassification is supported")


def _local_binary_reclassify(raster=None, match=None, invert=None,
                             dtype=np.uint8):
    """
    Binary reclassification of input data. All cell values in
    a numpy array are reclassified as uint8 (boolean) based on
    whether they match or do not match the values of an input match
    array.
    :param obj raster: a Raster, GeoRaster, or related generator object
    :param list match: a list object of integers specifying match values for reclassification
    """
    # args[0]/raster=
    if raster is None:
        raise IndexError("invalid raster= argument supplied by user")
    # args[1]/match=
    if match is None:
        raise IndexError("invalid match= argument supplied by user")
    # args[2]/invert=
    if invert is None:
        # this is an optional arg
        invert = False
    # if this is a Raster object, just drop
    # raster down to a GeoRaster and pass on
    if isinstance(raster, Raster):
        raster = raster.to_georaster()
    # if this is a complete GeoRaster, try
    # to process the whole object
    if isinstance(raster, GeoRaster):
        raster = raster.raster
        return np.reshape(
            np.array(
                np.in1d(raster, match, assume_unique=True, invert=invert),
                dtype=dtype
            ),
            raster.shape
        )
    # if this is a big raster that we've split into chunks
    # process this piece-wise
    elif isinstance(raster, types.GeneratorType):
        return np.concatenate(
            [np.reshape(
                np.array(
                    np.in1d(d[0], match, assume_unique=True, invert=invert),
                    dtype=dtype
                ),
                (1, d.shape[1])  # array shape tuple e.g., (1,1111)
             )
             for i, d in enumerate(raster)]
        )
    else:
        raise ValueError("raster= input should be a Raster, GeoRaster, or"
                         "Generator that numpy can work with")


def _local_reclassify(*args):
    raise NotImplementedError


def _local_crop(raster=None, shape=None, *args):
    """
    Wrapper for georasters.clip that will preform a crop operation on
    input raster
    """
    # args[0] / raster=
    if raster is None:
        raise IndexError("invalid raster= argument specified")
    # args[1] / shape=
    if shape is None:
        raise IndexError("invalid shape=argument specified")
    # sanity check and then do our crop operation
    # and return to user
    _enough_ram = _local_ram_sanity_check(raster.array)
    if not _enough_ram['available'] and not raster._use_disc_caching:
        logger.warning("There doesn't apprear to be enough free memory"
                       " available for our raster operation. You should use"
                       "disc caching options with your dataset. Est Megabytes "
                       "needed: %s", -1 * _enough_ram['bytes'] * 1E-07)
    return raster.to_georaster().clip(shape)



def _local_clip(raster=None, shape=None):
    """ 
    Wrapper for a crop operation 
    """
    # args[0]/raster=
    if raster is None:
        raise IndexError("invalid raster= argument specified")
    # args[1]/shape=
    if shape is None:
        raise IndexError("invalid shape= argument specified")
    return _local_crop(raster=raster, shape=shape)

def _local_extract(*args):
    """ 
    Local raster extraction handler
    :param args:
    :return:
    """
    pass

def _local_reproject(*args):
    pass


def _local_merge(rasters=None):
    """ 
    Wrapper for georasters.merge that simplifies merging raster segments
    returned by parallel operations.
    """
    if rasters is None:
        raise IndexError("invalid raster= argument specified")
    return merge(rasters)

def _local_split(raster=None, n=None):
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


def _local_ram_sanity_check(array=None):
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
    """ Determines the amount of free ram available for an operation. This is
    typically used in conjunction with _est_array_size() or as a precursor
    to raising MemoryError when working with large raster datasets
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


def _local_process_array_as_blocks(*args):
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

def _is_existing_file(*args):
    """
    :param args:
    :return:
    """
    raise NotImplementedError

def _is_wkt_str(wkt, *args):
    """
    Returns a boolean if a user-provided string can be parsed by GDAL as WKT

    :param str wkt: A GDAL-formatted WKT string; e.g., that is can be used to open rasters on a SQL server.
    :return: Returns true if the wkt str appear valid
    :rtype: Boolean
    """
    if args is None:
        args = {}
    wkt = args[0].get('wkt', None)

    raise NotImplementedError


class Gdal(object):
    def __init__(self, file, wkt, use_disc_caching, dtype, ndv, x_size, y_size, *args):
        """
        Wrapper for gdal primatives to fetch raster data from a file 
        or SQL database through WKT strings

        :param str file: Full path to a raster file you'd like to open.
        :param str wkt: A GDAL-formatted WKT string; e.g., that can be used to open rasters on a SQL server.
        :param bool use_disc_caching: Should we attempt to read our raster into RAM or should we cache it to disc? 
        """
        file = args[0].get('file', None)
        wkt = args[0].get('wkt', None)
        dtype = args[0].get('dtype', _DEFAULT_DTYPE)
        ndv = args[0].get('ndv', _DEFAULT_NA_VALUE)
        x_size = args[0].get('x_size', None)
        y_size = args[0].get('y_size', None)
        use_disc_caching = args[0].get('use_disc_caching', False)

        if use_disc_caching is not None:
            self._use_disc_caching = str(randint(1, 9E09)) + \
                                       '_np_array.dat'

        self.open_file()
        
    def open_file(self):
        """
        Read a raster file from disc as a formatted numpy array
        :rval none:
        """
        # grab raster meta information from GeoRasters
        try:
            self._ndv, self._x_size, self._y_size, self._geot, self._projection, self._dtype = \
                get_geo_info(self._filename)
        except Exception:
            raise AttributeError("problem processing file input -- is this"
                " a raster file?")
        # call gdal with explicit type specification
        # that will store in memory or as a disc cache, depending
        # on the state of our _use_disc_caching property
        if self._use_disc_caching is None:
            # create a cache file
            self.array = np.memmap(
                filename=self._use_disc_caching, dtype=self._dtype, mode='w+',
                shape = (self._y_size, self._x_size))
            # load file contents into the cache
            self.array[:] = gdalnumeric.LoadFile(
                filename=self._filename,
                buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(_to_numpy_type(self._dtype))
            )[:]
        # by default, load the whole file into memory
        else:
            self.array = gdalnumeric.LoadFile(
                filename=self._filename,
                buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(_to_numpy_type(self._dtype))
            )
        # make sure we honor our no data value
        self.array = np.ma.masked_array(
            self.array,
            mask=self.array == self._ndv,
            fill_value=self._ndv
        )

    def read_table(self, *args):
        connection = PostGis(args[0]).to_wkt
        raise NotImplementedError

class Raster(object):
    """ Raster class is a wrapper for generating GeoRasters,
    Numpy arrays, and Earth Engine Image objects. It opens files
    and converts to other formats as needed for various backend
    actions associated with Do.
    :arg file string specifying the full path to a raster
    file (typically a GeoTIFF) or an asset id for earth engine
    :return None
    """

    def __init__(self, filename=None, array=None, dtype=None,
                 disc_caching=None):
        self._backend = "local"
        self._array = None
        self._filename = None
        self._use_disc_caching = None  # Use mmcache? 

        self.ndv = _DEFAULT_NA_VALUE     # no data value
        self._xsize = None               # number of x cells (meters/degrees)
        self._ysize = None               # number of y cells(meters/degrees)
        self.geot = None                 # geographic transformation
        self.projection = None           # geographic projection
        self.dtype = None
        # args[0]/filename=
        self.filename = filename
        # args[1]/array=
        self.array = array
        # args[2]/dtype=
        if dtype is not None:
            self.dtype = dtype
        # args[3]/disc_cache=
        if disc_caching is not None:
            self._use_disc_caching = str(randint(1, 9E09)) + \
                                       '_np_binary_array.dat'
        # if we were passed a file argument, assume it's a
        # path and try to open it
        if self.filename is not None:
            try:
                self.open_file(self.filename)
            except OSError:
                raise OSError("couldn't open the filename provided")

    def __copy__(self):
        _raster = Raster()
        _raster._array = copy(self._array)
        _raster._backend = copy(self._backend)
        _raster._filename = copy(self._filename)
        _raster._use_disc_caching = copy(self._filename)
        _raster.ndv = self.ndv
        _raster._xsize = self._xsize
        _raster._ysize = self._ysize
        _raster.geot = self.geot
        _raster.projection = self.projection
        # if we are mem caching, generate a new tempfile
        return _raster

    def __deepcopy__(self, memodict={}):
        return self.__copy__()

    @property
    def array(self):
        return self._array

    @array.setter
    def array(self, *args):
        """
        Assign a numpy masked array to our Raster object
        """
        if args[0] is None:
            self._array = None
        else:
            try:
                self.dtype = args[0].dtype
            except AttributeError as e:
                logger.debug("array= expects a (masked) numpy array as input : %s -- skipping "
                "setting dtype and just loading the array", e)
            self._array = np.ma.masked_array(
                args[0],
                mask=self._array == self.ndv,
                fill_value=self.ndv,
                dtype=self.dtype)

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, *args):
        self._filename = args[0]

    @property
    def backend(self):
        return self._backend

    @backend.setter
    def backend(self, *args):
        self._backend = args[0]

    def open_file(self, file, dtype, *args):
        """ Open a local file handle for reading and assignment
        :param file:
        :return: None
        """
        if args is None:
            args = {}

        file = args[0].get('file', None)
        dtype = args[0].get('dtype', _DEFAULT_DTYPE)
        
        if file is None:
            raise IndexError("invalid file= data")

        # grab raster meta information from GeoRasters
        try:
            self.ndv, self._xsize, self._ysize, self.geot, self.projection, _dtype = \
                get_geo_info(file)
        except Exception:
            raise AttributeError("problem processing file input -- is this"
                " a raster file?")
        # args[1]/dtype=
        if dtype is None:
            # if the user didn't specify a type, just assume
            # the default 
            self.dtype = _dtype
        # re-cast our datatype as a numpy type, if needed
        if type(self.dtype) == str:
            self.dtype = _to_numpy_type(self.dtype)
        self.ndv = _no_data_value_sanity_check(self)
        # low-level call to gdal with explicit type specification
        # that will store in memory or as a disc cache, depending
        # on the state of our _use_disc_caching property
        if self._use_disc_caching is not None:
            # create a cache file
            self.array = np.memmap(
                self._use_disc_caching, dtype=self.dtype, mode='w+',
                shape = (self._xsize, self._ysize))
            # load file contents into the cache
            self.array[:] = gdalnumeric.LoadFile(
                filename=self.filename,
                buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(self.dtype))[:]
        # by default, load the whole file into memory
        else:
            self.array = gdalnumeric.LoadFile(
                filename=self.filename,
                buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(self.dtype)
            )
        # make sure we honor our no data value
        self.array = np.ma.masked_array(
            self.array,
            mask=self.array == self.ndv,
            fill_value=self.ndv
        )

    def write(self, filename=None, datatype=None, driver=_DEFAULT_RASTER_FORMAT):
        """ Wrapper for GeoRaster's create_geotiff that writes a numpy array to
        disk.
        :param filename:
        :param format:
        :param driver:
        :return:
        """
        if datatype is None:
            datatype = gdal_array.NumericTypeCodeToGDALTypeCode(self.dtype)
        if not filename:
            filename = self.filename.replace(".tif", "")
        try:
            create_geotiff(
                name=filename,
                Array=self.array,
                geot=self.geot,
                projection=self.projection,
                datatype=datatype,
                driver=driver,
                ndv=self.ndv,
                xsize=self._xsize,
                ysize=self._ysize)
            logger.debug("write() : write succeeded")
        except Exception as e:
            logger.debug("write() : general failure attempting to write raster to disk : %s", e)
            raise(e)

    def to_numpy_array(self):
        """ Returns numpy array values for our Raster object.
        :return:
        """
        return self.array

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

    def to_ee_image(self):
        """ Parses our internal numpy array as an Earth Engine ee.array object.
        Would like to see this eventually become a standard interface for
        dynamically ingesting raster data on Earth Engine, but it's currently
        broken
        """
        raise NotImplementedError
        #return ee.array(self.array)

def _to_numpy_type(user_str=None):
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

class RasterReimplementation(object):
    def __init__(self, input, host, port, username, password, *args):
        self.array = []
        self.crs = []
        self.crs_wkt = []

        input = args[0].get('input', None)
        port = args[0].get('port', None)
        username = args[0].get('username', None)
        password = args[0].get('password', None)
        
        self._builder({'input':input, 'port':port, 'username':username, 'password':password})

    def _builder(self, *args):
        if _is_existing_file(args[0]['input']):
            return Gdal(file=args[0]['input'])
        elif _is_wkt_str(args[0]['input']):
            return Gdal(wkt=args[0]['input'])