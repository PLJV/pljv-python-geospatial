#!/usr/bin/env python3

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2017, Playa Lakes Joint Venture"
__credits__ = "Kyle Taylor"
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

import logging

logger = logging.getLogger(__name__)

import os
import re
import numpy as np

from .raster import Raster
from scipy import ndimage


_DEFAULT_WRITE_ACTION = False
_DEFAULT_OVERWRITE_ACTION = False
_DEFAULT_DTYPE = np.float
_DEFAULT_FOOTPRINT_DTYPE = np.uint8 # this is typically boolean, formatted as integers

def gen_circular_array(nPixels=None, dtype=np.bool):
    """ 
    Make a 2-d array for buffering. It represents a circle of
    radius buffsize pixels, with 1 inside the circle, and zero outside.
    """
    kernel = None
    if nPixels > 0:
        n = 2 * nPixels + 1
        (r, c) = np.mgrid[:n, :n]
        radius = np.sqrt((r-nPixels)**2 + (c-nPixels)**2)
        kernel = (radius <= nPixels).astype(dtype)
    return kernel

def _dict_to_mwindow_filename(key, window_size):
    """
    Quick kludging to generate a filename from key + window size 
    """
    return str(key)+"_"+str(window_size)+"x"+str(window_size)

def ndimage_filter(image=None, outfile=None, **kwargs):
    """
    Wrapper for ndimage filters that can comprehend a GeoRaster,
    apply a common rcular buffer, and optionally writes a numpy array to
    disk following user specifications
    """
    if not args:
        args = {}

    _write = kwargs.get('write', _DEFAULT_WRITE_ACTION)
    _footprint = kwargs.get('footprint', None)
    _overwrite = kwargs.get('overwrite', _DEFAULT_OVERWRITE_ACTION)
    _function = kwargs.get('function', None)
    _size = kwargs.get('size', None)
    _intermediate_dtype = kwargs.get('intermediate_dtype', _DEFAULT_DTYPE),
    _dtype = kwargs.get('dtype', _DEFAULT_DTYPE)

    try:
        if outfile is None:
            write = False
        else:
            write = not os.path.isfile(outfile) | _overwrite & _write
            logger.debug("Will attempt to write raster file to dir: %s as %s", os.getcwd(), _outfile)
    except TypeError as e:
        logger.debug("Encountered an issue specifying a write file; "
            "filter will return result to user : %s", e)
        write = False
    
    try:
        _xsize = image._xsize
        _ysize = image._ysize
    except AttributeError:
        logger.debug("We were passed a numpy array without any cell sizes; "
            "disabling write calls and returning result to user.")
        write = False

    # format our Raster object as a numpy array
    
    if _size is not None:
        _footprint = gen_circular_array(_size, dtype=_dtype)
    try:
        # re-cast our user-provided, masked array with a zero fill-value
        image.array = np.ma.masked_array(
            image.array,
            fill_value=0,
            dtype=_intermediate_dtype)
        # and fill the numpy array with actual zeros before doing a moving windows analysis
        image = np.ma.filled(
            image.array,
            fill_value=0)
    except AttributeError as e:
        # otherwise, assume the user already supplied a properly formatted np.array
        pass
    
    # apply ndimage filter to user specifications
    # these can be used for the most common functions
    # we may encounter for moving windows analyses

    image = ndimage.generic_filter(
        input=image,
        function=_function,
        footprint=_footprint)

    logger.debug("Filter result : \n\n%s\n", image)
    logger.debug("Cumulative sum : %s", image.cumsum())
    
    # either save to disk or return to user
    if write:
        r = Raster(array = np.array(image))
        r._xsize = _xsize
        r._ysize = _ysize
        r.filename = outfile
        try:
            r.write()
        except AttributeError as e:
            r.write(filename=outfile)
        except Exception as e:
            logger.debug("%s doesn't appear to be a Raster object; "
                           "returning result to user", e)
            return image
    else:
        return image
