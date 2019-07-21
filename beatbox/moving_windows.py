#!/usr/bin/env python3

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2017, Playa Lakes Joint Venture"
__credits__ = "Kyle Taylor"
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

import os
import re
import numpy as np
import logging

from .raster import Raster

from scipy import ndimage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_DEFAULT_WRITE_ACTION = False
_DEFAULT_OVERWRITE_ACTION = False
_DEFAULT_DTYPE = np.float
_DEFAULT_FOOTPRINT_DTYPE = np.uint8 # this is typically boolean, formatted as integers

def gen_circular_array(nPixels=None, dtype=np.bool):
    """ make a 2-d array for buffering. It represents a circle of
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
    """ quick kludging to generate a filename from key + window size """
    return str(key)+"_"+str(window_size)+"x"+str(window_size)

def ndimage_filter(image, filename, write, footprint, overwrite, function, size, intermediate_dtype, dtype,*args):
    """ wrapper for ndimage filters that can comprehend a GeoRaster,
    apply a common rcular buffer, and optionally writes a numpy array to
    disk following user specifications
    """
    if not args:
        args = {}

    image = args.get('image', None)
    filename = args.get('filename', None)
    write = args.get('write', _DEFAULT_WRITE_ACTION)
    footprint = args.get('footprint', None)
    overwrite = args.get('overwrite', _DEFAULT_OVERWRITE_ACTION)
    function = args.get('function', None)
    size = args.get('size', None)
    intermediate_dtype = args.get('intermediate_dtype', _DEFAULT_DTYPE),
    dtype = args.get('dtype', _DEFAULT_DTYPE)

    # figure out if we are writing to disk

    try:
        if filename is None:
            write = False
        else:
            write = not os.path.isfile(filename) | overwrite & write
            logger.debug("Will attempt to write raster file to dir: %s as %s", os.getcwd(), filename)
    except TypeError as e:
        logger.debug("Encountered an issue specifying a write file -- "
            "filter will return result to user : %s", e)
        write = False
    
    try:
        _xsize = image._xsize
        _ysize = image._ysize
    except AttributeError:
        logger.debug("We were passed a numpy array without any cell sizes... "
            "disabling write calls and returning result to user.")
        write = False

    # format our Raster object as a numpy array
    
    if size:
        footprint = gen_circular_array(size, dtype=dtype)
    try:
        # re-cast our user-provided, masked array with a zero fill-value
        image.array = np.ma.masked_array(
            image.array,
            fill_value=0,
            dtype=intermediate_dtype)
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
        function=function,
        footprint=footprint)

    logger.debug("Filter result : \n\n%s\n", image)
    logger.debug("Cumulative sum : %s", image.cumsum())
    
    # either save to disk or return to user
    if write:
        outfile = Raster(array = np.array(image))
        outfile._xsize = _xsize
        outfile._ysize = _ysize
        outfile.filename = filename
        try:
            outfile.write()
        except AttributeError as e:
            outfile.write(filename=filename)
        except Exception as e:
            logger.debug("%s doesn't appear to be a Raster object; "
                           "returning result to user", e)
            return image
    else:
        return image
