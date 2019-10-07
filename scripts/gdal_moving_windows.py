#!/usr/bin/python3
"""
__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2017, Playa Lakes Joint Venture"
__credits__ = ["Kyle Taylor", "Alex Daniels"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"
"""

import sys
import re
from copy import copy

import logging
import warnings
import enlighten

logging.basicConfig(level=logging.DEBUG)

warnings.filterwarnings("ignore")

import numpy as np
import argparse as ap

from beatbox.raster import Raster
from beatbox.moving_windows import ndimage_filter

warnings.filterwarnings("default")

# define handlers for argparse for any arguments passed at runtime
example_text = str(
    "example: " + sys.argv[0] +
    " -r nass_2016.tif --reclass row_crop=1,2," +
    "3;wheat=2,7 -w 3,11,33 --fun numpy.sum")

descr_text = str(
    'A command-line interface for performing moving windows analyses' +
    ' on raster datasets using GDAL and numpy arrays')

parser = ap.ArgumentParser(
    prog='gdal_moving_windows.py',
    description=descr_text,
    epilog=example_text,
    formatter_class=ap.RawDescriptionHelpFormatter
)

parser.add_argument(
    '-r',
    '--raster',
    help='Specifies the full path to source raster file to use',
    type=str,
    required=True
)

parser.add_argument(
    '-c',
    '--reclass',
    help='If we are going to reclassify the input raster here are the cell values to match',
    type=str,
    required=False
)

parser.add_argument(
    '-f',
    '--fun',
    help='Specifies the function to apply over a moving window. The default function is sum. Sum, mean, and sd are supported.',
    type=str,
    required=True
)

parser.add_argument(
    '-w',
    '--window-sizes',
    help='Specifies the dimensions for our window(s)',
    type=str,
    required=True
)

parser.add_argument(
    '-s',
    '--shape',
    help='Specifies the shape parameters for your moving window. Currently \'square\', \'circle\', or \'gaussian\' are supported'
)

parser.add_argument(
    '-t',
    '--target-value',
    default=1,
    help='Specifies the target value we are reclassifying to, if the user asked us to reclassify. Default is binary reclassification',
    type=str,
    required=False
)

parser.add_argument(
    '-dt',
    '--dtype',
    help='Specifies the target data type for our moving windows analaysis',
    type=str,
    default=np.float32,
    required=False
)

parser.add_argument(
    '-o',
    '--outfile',
    help='Specify an output filename to use. If multiple window sizes are specified'+
    ' they are appended to the filename specified here.',
    type=str,
    required=False
)

parser.add_argument(
    '-d',
    '--debug',
    help='Enable verbose logging interface for debugging',
    action='store_true',
    default=False,
    required=False
)

args = vars(parser.parse_args())

# -d/--debug
if not args['debug']:
    # disable logging unless asked by the user
    logger = logging.getLogger(__name__)
    logger.disabled = True
else:
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.disabled = False

# standard numpy functions that we may have
# non-generic ndimage filters available for
# specifying these in advance can really
# speed-up our calculations

_NUMPY_STR_TO_FUNCTION = {
    'sum' : np.sum,
    'mean': np.mean,
    'median' : np.median,
    'sd' : np.std,
    'stdev' : np.std
}

def _to_numpy_function(user_fun_str=None):
    """
    Parse our NUMPY_STR dictionary using regular expressions
    for our user-specified function string.
    :return:
    """
    user_fun_str = str(user_fun_str).lower()
    for valid_function_str in list(_NUMPY_STR_TO_FUNCTION.keys()):
        # user might pass a key with extra designators
        # (like np.mean, numpy.median) -- let's
        if bool(re.search(string=user_fun_str, pattern=valid_function_str)):
            return _NUMPY_STR_TO_FUNCTION[valid_function_str]
    # default case
    return None

if __name__ == "__main__":
    # required parameters
    _WINDOW_DIMS = []     # dimensions for moving windows calculations
    _MATCH_ARRAYS = {}    # used for reclass operations
    if len(sys.argv) == 1 :
        parser.print_help()
        sys.exit(0)
    # -t/--target-values
    # if we reclass a raster, what should we reclass to?
    logger.debug(str(args))
    args['target_value'] = list(map(int, str(args.get('target_value')).split(',')))
    # -f/--fun
    # function to apply over each window
    args['fun'] = _to_numpy_function(args.get('fun', None))
    # if this doesn't map as a numpy function, maybe it's something else
    # lurking in the global scope we can find?
    if not args['fun']:
        try:
            if re.search(string=args['fun'],pattern="\."):
                function = args['fun'].split(".")
                # assume user wants an explicit function : e.g., "numpy.min"
                args['fun']=getattr(globals()[function[0]],function[1])
            else:
                args['fun']=globals()[args['fun']]()
        except KeyError as e:
            raise KeyError("user specified function can't be mapped to numpy "
                           "or anything else : %s", e)
    # -w/--window-size
    args['window_sizes'] = list(map(int, args.get('window_sizes', None).split(',')))
    # -c/--reclass
    if args['reclass']:
        _SRC_VALUES = args['reclass'].split(";")
        for v in _SRC_VALUES:
            v = v.split("=")
            _MATCH_ARRAYS[v[0]] = list(map(int, v[1].split(",")))
    # -o/--outfile
    # output filename prefix
    if args['outfile']:
        args['outfile'] = args['outfile'].replace('.tif','')
        args['outfile'] = args.get('outfile') + "_mw"
    else:
        args['outfile'] = args['raster'].replace('.tif','') + "_mw"
    logger.debug("Using outfile name (root) : %s", args['outfile'])
    # sanity-check runtime input
    if not args['window_sizes']:
        raise ValueError("Moving window dimensions need to be specified using"
        "the -w argument at runtime. see -h for usage.")
    elif not args['raster']:
        raise ValueError("An input raster should be specified"
        "with the -r argument at runtime. see -h for usage.")
    # Process our raster file stepwise, per window-size
    r = Raster(args['raster'], dtype=args['dtype'])
    # Progress reporting for raster sequences
    SHOW_PROGRESS = not logger.disabled and len(args['window_sizes']) > 1
    if SHOW_PROGRESS:
        manager = enlighten.get_manager()
        progress = manager.counter(
            total=len(_MATCH_ARRAYS),
            desc='Processing Moving Windows',
            unit='window')
    # perform any re-classification requests prior to our ndimage filtering
    if _MATCH_ARRAYS:
        logger.INFO(
            "performing moving window analyses across "
            + len(_MATCH_ARRAYS) + " windows"
        )
        for m in _MATCH_ARRAYS:
            focal = copy(r)
            if _MATCH_ARRAYS[m] is not None:
                logger.debug("reclassifying input raster using match array: %s", m)
                focal.array= binary_reclassify(
                    array=focal.array,
                    match=_MATCH_ARRAYS[m]
                )
            for w in args['window_sizes']:
                f = str(args['outfile']+"_"+str(w)+"x"+str(w))
                logger.debug("applying moving_windows.filter to reclassed "
                    "array for window size: %s", w)
                ndimage_filter({
                    'image' : focal,
                    'function':args['fun'],
                    'size': w,
                    'filename': f,
                    'dtype': args['dtype'] })
                if SHOW_PROGRESS : progress.update()
    # otherwise just do our ndimage filtering
    else:
        for w in args['window_sizes']:
            f = str(args['outfile']+"_"+str(w)+"x"+str(w))
            logger.debug("applying moving_windows.filter to image "
                "array for window size: %s", w)
            output = copy(r)
            output.filename = f
            output.array = ndimage_filter({
                'image' : r,
                'function' : args['fun'],
                'size' : w,
                'dtype' : args['dtype'] })
            output.write()
            if SHOW_PROGRESS : progress.update()

