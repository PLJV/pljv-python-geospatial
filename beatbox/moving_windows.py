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

from .raster import Raster, NdArrayDiscCache
from scipy import ndimage


_DEFAULT_WRITE_ACTION = False
_DEFAULT_OVERWRITE_ACTION = False
_DEFAULT_DTYPE = np.float
_DEFAULT_FOOTPRINT_DTYPE = np.uint8  # this is typically boolean, formatted as integers


def gen_circular_array(num_pixels=None, dtype=np.bool):
    """ 
    Make a 2-d array for buffering. It represents a circle of
    radius buffsize pixels, with 1 inside the circle, and zero outside.
    """
    kernel = None
    if num_pixels > 0:
        n = 2 * num_pixels + 1
        (r, c) = np.mgrid[:n, :n]
        radius = np.sqrt((r - num_pixels) ** 2 + (c - num_pixels) ** 2)
        kernel = (radius <= num_pixels).astype(dtype)
    return kernel


def _dict_to_mwindow_filename(key, window_size):
    """
    Quick kludging to generate a filename from key + window size 
    """
    return str(key) + "_" + str(window_size) + "x" + str(window_size)


def ndimage_filter(array=None, **kwargs):
    """
    Wrapper for ndimage filters that can comprehend a GeoRaster,
    apply a common rcular buffer, and optionally writes a numpy array to
    disk following user specifications
    """
    kwargs["use_disc_caching"] = kwargs.get("use_disc_caching", False)
    kwargs["footprint"] = kwargs.get("footprint", None)
    kwargs["function"] = kwargs.get("function", None)
    kwargs["size"] = kwargs.get("size", None)
    kwargs["intermediate_dtype"] = (kwargs.get("intermediate_dtype", None),)
    kwargs["dtype"] = kwargs.get("dtype", array.dtype)

    kwargs["x_size"] = kwargs.get("x_size", array.shape[0])
    kwargs["y_size"] = kwargs.get("y_size", array.shape[1])

    # format our Raster object as a numpy array

    if kwargs["size"] is not None:
        kwargs["footprint"] = gen_circular_array(kwargs["size"])
    try:
        # re-cast our user-provided, masked array with a zero fill-value
        if kwargs["use_disc_caching"] is True:
            _array = NdArrayDiscCache(array)
            array = _array.array
        array = np.ma.masked_array(array, fill_value=0)
        # and fill the numpy array with actual zeros before doing a moving windows analysis
        array = np.ma.filled(array, fill_value=0)
    except AttributeError:
        # otherwise, assume the user already supplied a properly formatted np.array
        pass

    # apply ndimage filter to user specifications
    # these can be used for the most common functions
    # we may encounter for moving windows analyses

    if kwargs["use_disc_caching"] is True:
        array[:] = ndimage.generic_filter(
            input=array, function=kwargs["function"], footprint=kwargs["footprint"]
        )[:]
        _array.array = array
        return _array
    else:
        return ndimage.generic_filter(
            input=array, function=kwargs["function"], footprint=kwargs["footprint"]
        )
