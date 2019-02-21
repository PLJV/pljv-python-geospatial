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
from .do import _build_kwargs_from_args

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

def _dict_to_mwindow_filename(key=None, window_size=None):
    """ quick kludging to generate a filename from key + window size """
    return str(key)+"_"+str(window_size)+"x"+str(window_size)

def ndimage_filter(*args, **kwargs):
    """ wrapper for ndimage filters that can comprehend a GeoRaster,
    apply a common rcular buffer, and optionally writes a numpy array to
    disk following user specifications
    """
    KNOWN_ARGS = ['image', 'filename', 'write', 'footprint', 'overwrite', 
                  'function', 'size', 'intermediate_dtype', 'dtype']
    DEFAULTS = [None, None, _DEFAULT_WRITE_ACTION, None, 
        _DEFAULT_OVERWRITE_ACTION, None, None, _DEFAULT_DTYPE, 
        _DEFAULT_DTYPE]
    if len(args) > 0:
        if type(args[0]) == dict:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)

    # figure out if we are writing to disk

    try:
        if 'filename' not in kwargs.keys():
            kwargs['filename'] = None
            kwargs['write'] = False
        else:
            kwargs['write'] = not os.path.isfile(kwargs['filename']) | kwargs['overwrite'] & kwargs['write']
            logger.debug("Will attempt to write raster file to dir: %s as %s", os.getcwd(), kwargs['filename'])
    except TypeError as e:
        logger.debug("Encountered an issue specifying a write file -- "
            "filter will return result to user : %s", e)
        kwargs['write'] = False
    
    # grab our x/y cell sizes, if they are available
    
    try:
        kwargs['xsize'] = kwargs['image'].xsize
        kwargs['ysize'] = kwargs['image'].ysize
    except AttributeError:
        logger.debug("We were passed a numpy array without any cell sizes... "
            "disabling write calls and returning result to user.")
        kwargs['write'] = False

    # format our Raster object as a numpy array
    
    if kwargs['size']:
        kwargs['footprint'] = gen_circular_array(kwargs['size'], dtype=_DEFAULT_FOOTPRINT_DTYPE)
    try:
        # re-cast our user-provided, masked array with a zero fill-value
        kwargs['image'].array = np.ma.masked_array(
            kwargs['image'].array,
            fill_value=0,
            dtype=kwargs['intermediate_dtype'])
        # and fill the numpy array with actual zeros before doing a moving windows analysis
        kwargs['image'] = np.ma.filled(
            kwargs['image'].array,
            fill_value=0)
    except AttributeError as e:
        # otherwise, assume the user already supplied a properly formatted np.array
        pass
    
    logger.debug("Ndimage filter() run-time parameters : \n%s\n", str(kwargs))
    
    # apply ndimage filter to user specifications
    # these can be used for the most common functions
    # we may encounter for moving windows analyses

    kwargs['image'] = ndimage.generic_filter(
        input=kwargs['image'],
        function=kwargs['function'],
        footprint=kwargs['footprint'])

    logger.debug("Filter result : \n\n%s\n", kwargs['image'])
    logger.debug("Cumulative sum : %s", kwargs['image'].cumsum())
    
    # either save to disk or return to user
    if kwargs['write']:
        outfile = Raster(array = np.array(kwargs['image']))
        outfile.xsize = kwargs['xsize']
        outfile.ysize = kwargs['ysize']
        outfile.filename = kwargs['filename']
        try:
            outfile.write()
        except AttributeError as e:
            outfile.write(filename=kwargs['filename'])
        except Exception as e:
            logger.debug("%s doesn't appear to be a Raster object; "
                           "returning result to user", e)
            return kwargs['image']
    else:
        return kwargs['image']
