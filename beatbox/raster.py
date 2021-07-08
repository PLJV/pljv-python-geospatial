#!/usr/bin/env python3
"""
Wrappers for gdal, rasterio, and georasters useful for manipulating raster data
"""
__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2019"
__credits__ = ["Kyle Taylor"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.a.taylor@gmail.com"
__status__ = "Testing"
# logging
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# mmap file caching and file handling
import sys
import os
import subprocess
import re
from random import randint
from copy import copy
from pathlib import Path

# raster manipulation
from georasters import GeoRaster, get_geo_info, create_geotiff, merge
import rasterio as rio
import gdalnumeric
import gdal
import numpy as np
from osgeo import gdal_array

# memory profiling
import types
import psutil

# beatbox
from .network import PostGis

gdal.UseExceptions()

_DEFAULT_NA_VALUE = 65535
_DEFAULT_DTYPE = np.uint16
_DEFAULT_RASTER_FORMAT = gdal.GetDriverByName("GTiff")

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
    "complex128": np.complex128,
}


def geotransform_to_affine(geot):
    """
    Convert GDAL geo transform to affine, which is used more commonly
    for specifying the configuration of raster datasets in newer 
    frameworks like rasterio
    """
    c, a, b, f, d, e = list(geot)
    return rio.Affine(a, b, c, d, e, f)


def ram_sanity_check(array=None):
    """
    Determine if there is enough ram for an operation and return 
    the amount of ram available (in bytes) as a dictionary 
    :param np.array array: NumPy array 
    :return dict:
    """
    cost = est_free_ram() - est_array_size(array)
    return {"available": bool(cost > 0), "bytes": int(cost)}


def est_free_ram():
    """ Determines the amount of free ram available for an operation. 
    :return: int (free ram measured in bytes)
    """
    return psutil.virtual_memory().free


def est_array_size(obj=None, byte_size=None, dtype=None):
    """ Estimate the total size (in bytes) an array-like object will consume
    :param args:
    :return:
    """
    # args[0] is a list containing array dimensions
    if isinstance(obj, list) or isinstance(obj, tuple):
        array_len = np.prod(obj)
    elif isinstance(obj, GeoRaster):
        dtype = obj.datatype
        array_len = np.prod(obj.shape)
        byte_size = _to_numpy_type(obj.datatype)
    elif isinstance(obj, Raster):
        dtype = obj.array.dtype
        array_len = np.prod(obj.array.shape)
        byte_size = _to_numpy_type(obj.array.dtype)
    else:
        array_len = len(obj)

    if dtype is not None:
        byte_size = sys.getsizeof(_to_numpy_type(dtype))
    else:
        raise IndexError(
            "couldn't assign a default data type and an invalid"
            " dtype= argument specified"
        )
    return array_len * byte_size


def process_blockwise(*args):
    """
    Accepts an array object and splits it into chunks that can be handled
    stepwise
    :param np.array np.array: NumPy array object
    :return:
    """
    _array = args[0].raster  # numpy array
    _rows = _array.shape[0]  # rows in array
    _n_chunks = 1  # how many blocks (rows) per chunk?
    """Yield successive n-sized chunks from 0-to-nrow."""
    for i in range(0, _rows, _n_chunks):
        yield _array[i : i + _n_chunks]


def is_raster(obj=None):
    try:
        if isinstance(obj, Raster):
            return True
        else:
            return False
    except:
        return False


def is_array(obj=None):
    try:
        if isinstance(obj, np.ma.core.MaskedArray):
            return True
        else:
            return False
    except Exception:
        return False


def is_number(num_list=None):
    """
    Determine whether any item in a list is not a number.
    :param args[0]: a python list object
    :return: True on all integers,
    """
    try:
        if (
            np.sum([not (isinstance(i, int) or isinstance(i, float)) for i in num_list])
            > 0
        ):
            return False
        else:
            return True
    except ValueError:
        return False


def is_wkt_str(wkt=None, *args):
    """
    Returns a boolean if a user-provided string can be parsed by GDAL as WKT

    :param str wkt: A GDAL-formatted WKT string; e.g., that is can be used to open rasters on a SQL server.
    :return: Returns true if the wkt str appear valid
    :rtype: Boolean
    """
    raise NotImplementedError


def slope(array=None, use_disc_caching=True):
    if use_disc_caching is True:
        x, y, slp = (
            NdArrayDiscCache(array),
            NdArrayDiscCache(array),
            NdArrayDiscCache(array),
        )
        x.array[:], y.array[:] = np.gradient(array)[:]
        slp.array[:] = (
            np.pi / 2.0 - np.arctan(np.sqrt(x.array * x.array + y.array * y.array))[:]
        )
        del x, y
        return slp
    else:
        x, y = np.gradient(array)
        return np.pi / 2.0 - np.arctan(np.sqrt(x * x + y * y))


def aspect(array=None, use_disc_caching=True):
    if use_disc_caching is True:
        x, y, asp = (
            NdArrayDiscCache(array),
            NdArrayDiscCache(array),
            NdArrayDiscCache(array),
        )
        x.array[:], y.array[:] = np.gradient(array)[:]
        asp.array[:] = np.arctan2(-x.array, y.array)[:]
        del x, y
        return asp
    else:
        x, y = np.gradient(array)
        return np.arctan2(-x, y)


def array_split(array=None, n=None, **kwargs):
    """
    Wrapper for numpy array_split that can comprehend a Raster object.
    Splits an input array into n (mostly) equal segments, possibly for 
    a parallelized operation.
    :param np.array raster:
    :param int n: number of splits to use for np.array_split 
    :return:
    """
    _kwargs = {}

    if isinstance(array, Raster):
        _kwargs["ary"] = array.array
    else:
        _kwargs["ary"] = array

    _kwargs["indices_or_sections"] = n

    # args[0]/raster=
    if array is None:
        raise IndexError("invalid raster= argument specified")
    # args[1]/n=
    if n is None:
        raise IndexError("invalid n= argument specified")
    return np.array_split(**_kwargs)


def write_raster(array=None, filename=None, template=None, **kwargs):
    """
    Wrapper for rasterio that can write NumPy arrays to disc using an optional
    Raster template object
    """

    kwargs["driver"] = kwargs.get("driver", "GTiff")
    kwargs["dtype"] = kwargs.get("dtype", str(array.dtype))
    kwargs["width"] = kwargs.get("width", array.shape[0])
    kwargs["height"] = kwargs.get("height", array.shape[1])
    kwargs["count"] = kwargs.get("count", 1)
    kwargs["crs"] = kwargs.get("crs", None)
    kwargs["transform"] = kwargs.get("transform", None)

    if template is not None:
        kwargs["transform"] = geotransform_to_affine(template.geot)
        kwargs["crs"] = template.projection.ExportToProj4()
    if kwargs["crs"] is None:
        LOGGER.debug(
            "crs= was not specified and cannot be determined from a "
            + "numpy array; Resulting GeoTIFF will have no projection."
        )
    if kwargs["transform"] is None:
        LOGGER.debug(
            "transform= was not specified; Resulting GeoTIFF will "
            + "have an undefined affine transformation."
        )

    try:
        with rio.open(filename, "w", **kwargs) as dst:
            dst.write(array, 1)

        return True

    except FileNotFoundError:
        LOGGER.exception(
            "FileNotFoundError in filename= argument of write_raster():"
            + "This should not happen -- are you writing to a weird dir?"
        )
        return False

    return False


def parse_band_descriptions(path):
    """
    Wrapper for gdal info that will parse the DESCRIPTION field of a
    multi-band raster and return band names as an ordered list object
    """
    band_names = [
        x for x in gdal.Info(path).split("\n") if re.search("Description =", x)
    ]

    return [b_str.split("= ")[1] for b_str in band_names]


def append_prefix_to_filename(full_path, prefix=None):
    """
    Shorthand for Path.parts that accepts a full path to a file
    and will append some user-specified prefix to the filename.
    Returns modified full path as a Path object
    """

    if prefix is None:
        prefix = "temp"

    file_name = Path(full_path).name

    root = Path(full_path).parts

    if ":" in root[0]:
        root = root[0] + "/".join(root[1 : len(root) - 1])
    else:
        root = root[0] + "/" + "/".join(root[1 : len(root) - 1])

    dest = prefix + "_" + file_name
    dest = Path(root + "/" + dest)

    return dest


def get_extent_params(full_path=None):
    """
    Fetches odds-and-ends about spatial configuration from
    a raster image file
    """

    if full_path is None:
        raise AttributeError("No full_path= argument supplied by user.")

    full_path = str(Path(full_path))

    image_file = gdal.Open(full_path)

    upx, xres, xskew, upy, yskew, yres = image_file.GetGeoTransform()

    # round to prevent strange pixel shifting in Albers --
    # but note that 5 digits of precision still allows for specificity
    # when working with decimal degrees
    xres, yres = (round(xres, 5), round(yres, 5))

    prj = image_file.GetProjection()

    cols = image_file.RasterXSize
    rows = image_file.RasterYSize

    ulx = upx + 0 * xres + 0 * xskew
    uly = upy + 0 * yskew + 0 * yres

    llx = upx + 0 * xres + rows * xskew
    lly = upy + 0 * yskew + rows * yres

    lrx = upx + cols * xres + rows * xskew
    lry = upy + cols * yskew + rows * yres

    urx = upx + cols * xres + 0 * xskew
    ury = upy + cols * yskew + 0 * yres

    return {
        "crs": prj,
        "xy_res": [xres, yres],
        # (minX, minY, maxX, maxY)
        "bounding_box": [
            min(ulx, llx, lrx, urx),
            min(uly, lly, lry, ury),
            max(ulx, llx, lrx, urx),
            max(uly, lly, lry, ury),
        ],
        # for use with some instances of gdal.Translate
        "corners": [ulx, uly, lrx, lry],
        "transform": [upx, xres, xskew, upy, yskew, yres],
    }


def snap_geotransform(source_file=None, using=None, force_ul_corner=True):
    """
    Apply an X/Y offset to geotransform parameters extracted
    from source= using the number of columns (X) and rows (Y)
    in a to= raster. This is a conveinience function needed for 
    'snapping' smaller source raster tiles to the extent of a 
    larger raster grid.
    """

    # i.e., (smaller) source raster
    source_gt = get_extent_params(source_file)["transform"]

    # i.e., (larger) raster file to pull geo-transform parameters from
    snap_gt = get_extent_params(using)["transform"]

    x_off = int(source_gt[0] - snap_gt[0])
    y_off = int(source_gt[3] - snap_gt[3])

    if force_ul_corner:
        # older software (ArcInfo) might not see
        # the UL x,y coords as a centroid. This
        # adjustment will force the x,y coordinates
        # at the origin from center-of-pixel alignment
        # to true-upper-left alignment
        x_off = x_off - (0.5 * snap_gt[1])
        y_off = y_off - (0.5 * snap_gt[5])

    # return the geotransform parameters of our to (snap) grid,
    # with the x/y coordinates of our UL pixel moved using our
    # offset estimate
    return [
        snap_gt[0] + x_off,
        snap_gt[1],
        snap_gt[2],
        snap_gt[3] + y_off,
        snap_gt[4],
        snap_gt[5],
    ]


def set_geotransform(destination_file=None, source_file=None):
    """
    Wrapper for SetGeoTransform that will open a target "to" file
    and apply geotranform parameters take from a "from" file (e.g., 
    an SDL snap layer). Assumes that gdalwarp has been called before
    applying the geotransform.
    """

    if not isinstance(source_file, list):
        # if source isn't a list, assume it's a raster file containing
        # are target geotransform parameters
        geotransform = get_extent_params(source_file)["transform"]
    else:
        # if a list was provided, assume it's a an explicit list of
        # geotransform parameters
        geotransform = source_file

    try:
        ds = gdal.Open(str(destination_file), gdal.GA_Update)
        ds.SetGeoTransform(geotransform)
        ds = None  # close our file handle
    except Exception:
        LOGGER.info(
            "Warning:Error setting geotransform for file:" + str(destination_file)
        )

    del ds


def define_projection(image, proj_file):
    """
    Wrapper for gdalwarp that will force a raster file into a given projection using
    the WKT from a .prj file. This is roughly equivalent to "define projection" in ArcGIS and 
    is only used because Earth Engine doesn't always report the correct projection for
    exported assets
    """
    temp_file = append_prefix_to_filename(image, "reprojected")

    command_args = [
        str(sys.executable).replace(
            "python", "gdalwarp"
        ),  # full path to gdal_warp (from gdal_merge)
        "-s_srs",
        str(Path(proj_file)),
        "-t_srs",
        str(Path(proj_file)),
        "-overwrite",
        "-q",
        str(Path(image)),  # source
        str(Path(temp_file)),  # destination
    ]

    res = subprocess.call(command_args, shell=False)

    if res != 0:
        LOGGER.info("Gdalwarp encountered a runtime error for dest=" + str(image))

    shutil.move(temp_file, image)

    return Path(image)


def snap_geotransform(source_file=None, using=None, force_ul_corner=True):
    """
    Apply an X/Y offset to geotransform parameters extracted
    from source= using the number of columns (X) and rows (Y)
    in a to= raster. This is a conveinience function needed for 
    'snapping' smaller source raster tiles to the extent of a 
    larger raster grid.
    """

    # i.e., (smaller) source tile
    source_gt = get_extent_params(source_file)["transform"]

    # i.e., (larger) snap grid
    snap_gt = get_extent_params(using)["transform"]

    x_off = int(source_gt[0] - snap_gt[0])
    y_off = int(source_gt[3] - snap_gt[3])

    if force_ul_corner:
        # older software (ArcInfo) might not see
        # the UL x,y coords as a centroid. This
        # adjustment will force the x,y coordinates
        # at the origin from center-of-pixel alignment
        # to true-upper-left alignment
        x_off = x_off - (0.5 * snap_gt[1])
        y_off = y_off - (0.5 * snap_gt[5])

    # return the geotransform parameters of our to (snap) grid,
    # with the x/y coordinates of our UL pixel moved using our
    # offset estimate
    return [
        snap_gt[0] + x_off,
        snap_gt[1],
        snap_gt[2],
        snap_gt[3] + y_off,
        snap_gt[4],
        snap_gt[5],
    ]


def reproject(source_file, destination_file, extent_params):
    """
    Wrapper for gdalwarp that will reproject a source_file using the CRS string
    provided by the user via extent_params
    """
    command_args = [
        str(sys.executable).replace(
            "python", "gdalwarp"
        ),  # full path to gdal_warp (from gdal_merge)
        "-t_srs",
        extent_params["crs"],
        "-tr",
        extent_params["xy_res"][0],
        extent_params["xy_res"][1],
        "-tap",  # align our pixels with the source CRS
        "-overwrite",
        "-q",
        str(Path(source_file)),  # source
        str(Path(destination_file)),  # destination
    ]

    res = subprocess.call(command_args, shell=False)

    if res != 0:
        LOGGER.info(
            "Gdalwarp encountered a runtime error for dest="
            + str(destination_file)
            + " and src="
            + str(source_file)
        )

    return Path(destination_file)


class NdArrayDiscCache(object):
    def __init__(self, input=None, **kwargs):
        """
        ArrayDiscCache helps build a local numpy memmap file and either loads raster data from disc into the cache, 
        or attempts a direct assignment of a user-specific numpy array to a cache file (via the input= argument).
        :param str input: Either a full path to a raster file memmap'd to disk or an nd array object that is mapped directly
        :param str dtype: Numpy datatype specification to use for our array cache
        :param str x_size: Number of cells (x-axis) to use for our array cache
        :param str y_size: Number of cells (y-axis) to use for our array cache
        """
        self.disc_cache_file = os.path.abspath(
            kwargs.get("cache_filename", str(randint(1, 9e09)) + "_np_array.dat")
        )

        self.dtype = kwargs.get("dtype", _DEFAULT_DTYPE)

        self.x_size = kwargs.get("x_size", None)
        self.y_size = kwargs.get("y_size", None)

        self.ndv = kwargs.get("ndv", _DEFAULT_NA_VALUE)

        LOGGER.debug(
            "Using disc caching file for large numpy array: " + self.disc_cache_file
        )

        # if the user specified a valid file path or numpy array object, try to read it into our cache
        # but otherwise allow an empty specification
        if input is None:
            # by default, just create an empty cache file from the parameters specified by the user
            self.array = np.memmap(
                filename=self.disc_cache_file,
                dtype=_to_numpy_type(self.dtype),
                mode="w+",
                shape=(self.y_size, self.x_size),
            )
        else:
            if os.path.isfile(input):
                LOGGER.debug(
                    "Loading file contents into disc cache file:" + self.disc_cache_file
                )
                _raster_file = gdal.Open(input)
                self.array = np.memmap(
                    filename=self.disc_cache_file,
                    dtype=_to_numpy_type(self.dtype),
                    mode="w+",
                    shape=(_raster_file.RasterYSize, _raster_file.RasterXSize),
                )
                del _raster_file
                self.array[:] = gdalnumeric.LoadFile(
                    filename=input,
                    buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(
                        _to_numpy_type(self.dtype)
                    ),
                )[:]
            else:
                LOGGER.debug(
                    "Treating input= object as numpy array object and reading into disc cache file:"
                    + self.disc_cache_file
                )
                self.array = np.memmap(
                    filename=self.disc_cache_file,
                    dtype=input.dtype,
                    mode="w+",
                    shape=input.shape,
                )
                self.array[:] = input[:]


class Gdal(object):
    def __init__(self, **kwargs):
        """
        Wrapper for gdal primatives to fetch raster data from a file 
        or SQL database through WKT strings

        :param str file: Full path to a raster file you'd like to open.
        :param str wkt: A GDAL-formatted WKT string; e.g., that can be used to open rasters on a SQL server.
        :param bool use_disc_caching: Should we attempt to read our raster into RAM or should we cache it to disc? 
        """
        self.filename = kwargs.get("file")
        self.wkt = kwargs.get("wkt")
        self.dtype = kwargs.get("dtype", _DEFAULT_DTYPE)
        self.ndv = kwargs.get("ndv", _DEFAULT_NA_VALUE)
        self.x_size = kwargs.get("x_size")
        self.y_size = kwargs.get("y_size")
        self.geot = kwargs.get("geot")
        self.projection = kwargs.get("projection")
        self.use_disc_caching = kwargs.get("use_disc_caching", False)
        self.disc_cache_file = kwargs.get("disc_cache_file", None)

        # allow for an empty specification
        if self.filename is not None:
            self.open()
        elif self.wkt is not None:
            raise NotImplementedError("WKT string input is not supported yet")

    def open(self):
        """
        Read a raster file from disc as a formatted numpy array
        :rval none:
        """
        # grab raster meta informationP from GeoRasters
        try:
            self.ndv, self.x_size, self.y_size, self.geot, self.projection, self.dtype = get_geo_info(
                self.filename
            )
        except Exception:
            raise AttributeError(
                "problem processing file input -- is this" + " a raster file?"
            )
        # call gdal with explicit type specification
        # that will store in memory or as a disc cache, depending
        # on the state of our use_disc_caching property
        if self.use_disc_caching is True:
            # create a cache file and load file contents into the cache
            _cached_file = NdArrayDiscCache(
                input=self.filename,
                dtype=self.dtype,
                x_size=self.x_size,
                y_size=self.y_size,
            )
            self.array = _cached_file.array
            self.disc_cache_file = _cached_file.disc_cache_file
        # by default, load the whole file into memory
        else:
            self.array = gdalnumeric.LoadFile(
                filename=self.filename,
                buf_type=gdal_array.NumericTypeCodeToGDALTypeCode(
                    _to_numpy_type(self.dtype)
                ),
            )
        # make sure we honor our no data value
        self.array = np.ma.masked_array(
            self.array, mask=self.array == self.ndv, fill_value=self.ndv
        )

    def write(self, **kwargs):
        write_raster(
            array=self.array,
            filename=self.filename,
            template=kwargs.pop("template", None),
            **kwargs
        )

    def sql_read(self, *args):
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
    def __init__(self, input=None, **kwargs):
        """
        Raster class will intuit how to read raster data depending on the context of user-input.
        :param input: input string that can be a dictionary of named arguments, JSON, or a full-path to a file to read from disc.
        """
        self.array = []
        self.crs = []
        self.crs_wkt = []
        self.ndv = _DEFAULT_NA_VALUE
        self.projection = None
        self.dtype = _DEFAULT_DTYPE
        self.geot = None
        self.use_disc_caching = False
        self.disc_cache_file = None

        # allow for an empty specification by user
        if input is not None:
            self._builder(input, kwargs)

    def __del__(self):
        if self.use_disc_caching is True:
            LOGGER.debug(
                "Attempting removing local numpy disc caching file: "
                + self.disc_cache_file
            )
            if os.path.isfile(self.disc_cache_file):
                os.remove(self.disc_cache_file)

    def _builder(self, input=None, config={}):
        if os.path.isfile(input):
            _kwargs = {"file": input}
            _kwargs.update(config)
            _raster = Gdal(**_kwargs)
            self.array = _raster.array
        # elif _is_wkt_str(config.get('input')):
        #     _raster = Gdal(wkt=config.get('input'))
        elif is_array(input):
            _raster = Raster()
            _raster.array[:] = input[:]
        elif is_raster(input):
            _raster = input
            if _raster.use_disc_caching is True:
                _cached_file = NdArrayDiscCache(
                    input=_raster.array, dtype=_raster.dtype
                )
                self.array = _cached_file.array
                self.disc_cache_file = _cached_file.disc_cache_file
        else:
            # allow an empty specification
            _raster = Raster()
            self.array = _raster.array

        self.geot = _raster.geot
        self.ndv = _raster.ndv
        self.projection = _raster.projection
        self.dtype = _raster.dtype
        self.use_disc_caching = _raster.use_disc_caching
        self.disc_cache_file = _raster.disc_cache_file

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
            datatype=self.dtype,
        )

    def to_numpy_array(self):
        """ Returns numpy array values for our Raster object.
        :return:
        """
        return self.array
