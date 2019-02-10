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

import sys, os, re
import numpy as np
import argparse as ap
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from beatbox.raster import Raster
from beatbox.moving_windows import filter

# define handlers for argparse for any arguments passed at runtime
example_text = str(
    "example: " + sys.argv[0] +
    " -r nass_2016.tif --reclass row_crop=1,2," +
    "3;wheat=2,7 -w 3,11,33 --fun numpy.sum")
descr_text = str(
    'A command-line interface for performing moving windows analyses' +
    ' on raster datasets using GDAL and numpy arrays')

parser = ap.ArgumentParser(
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
    '-t',
    '--target-value',
    help='Specifies the target value we are reclassifying to, if the user asked us to reclassify. Default is binary reclassification',
    type=str,
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
    type=str,
    required=False
)

args = vars(parser.parse_args())

# -d/--debug
if not args['debug']:
    # disable logging unless asked by the user
    logger.disabled = True

# standard numpy functions that we may have
# non-generic ndimage filters available for
# specifying these in advance can really
# speed-up our calculations
_NUMPY_STR_TO_FUNCTIONS = {
    'sum' : np.sum,
    'mean': np.mean,
    'median' : np.median,
    'sd' : np.std,
    'stdev' : np.std
}

def get_numpy_function(user_fun_str=None):
    """
    Parse our NUMPY_STR dictionary using regular expressions
    for our user-specified function string.
    :return:
    """
    for np_function_str in list(_NUMPY_STR_TO_FUNCTIONS.keys()):
        # user might pass a key with extra designators
        # (like np.mean, numpy.median) -- let's
        if bool(re.search(string=str(user_fun_str), pattern=np_function_str)):
            return _NUMPY_STR_TO_FUNCTIONS[np_function_str]
    # default case
    return None

if __name__ == "__main__":
    # required parameters
    _INPUT_RASTER = None  # full-path to a raster file to apply our window over
    _FUNCTION = None    # function to apply over each window
    _WINDOW_DIMS = []   # dimensions for moving windows calculations
    _MATCH_ARRAYS = {}  # used for reclass operations
    _TARGET_RECLASS_VALUE = [1] # if we reclass a raster, what should we reclass to?
    _OUTFILE_NAME = "output" # output filename prefix
    if len(sys.argv) == 1 :
        parser.print_help()
        sys.exit(0)
    # -r/--raster
    _INPUT_RASTER = args['raster']
    # -t/--target-values
    if args['target_value']:
        _TARGET_RECLASS_VALUE = list(map(int, args['target_value'].split(',')))
    # -f/--fun
    _FUNCTION = get_numpy_function(args['fun'])
    # if this doesn't map as a numpy function, maybe it's something else
    # lurking in the global scope we can find?
    if not _FUNCTION:
        try:
            if re.search(string=args['fun'],pattern="\."):
                function = args['fun'].split(".")
                # assume user wants an explicit function : e.g., "numpy.min"
                _FUNCTION=getattr(globals()[function[0]],function[1])
            else:
                _FUNCTION=globals()[args['fun']]()
        except KeyError as e:
            raise KeyError("user specified function can't be mapped to numpy ",
                           "or anything else : %s", e)
    # -w/--window-size
    if args['window_sizes']:
        _WINDOW_DIMS = list(map(int, args['window_sizes'].split(',')))
    # -c/--reclass
    if args['reclass']:
        _classes = args['reclass'].split(";")
        for c in classes:
            c = c.split("=")
            _MATCH_ARRAYS[c[0]] = list(map(int, c[1].split(",")))
    # -o/--outfile
    if args['outfile']:
        _OUTFILE_NAME = args['outfile']
    # sanity-check runtime input
    if not _WINDOW_DIMS:
        raise ValueError("moving window dimensions need to be specified using"
        "the -w argument at runtime. see -h for usage.")
    elif not _INPUT_RASTER:
        raise ValueError("An input raster should be specified"
        "with the -r argument at runtime. see -h for usage.")
    # Process our raster file stepwise, per window-size
    r = Raster(_INPUT_RASTER)
    # Progress reporting for raster sequences
    if not logger.disabled:
        manager = enlighten.get_manager()
        progress = manager.counter(
            total=len(_MATCH_ARRAYS),
            desc='Rasters',
            unit='ticks')
    # perform any re-classification requests prior to our ndimage filtering
    if _MATCH_ARRAYS:
        logger.INFO(
            "performing moving window analyses across "
            + len(_MATCH_ARRAYS) + " windows"
        )
        for m in _MATCH_ARRAYS:
            focal = r
            if _MATCH_ARRAYS[m] is not None:
                logger.DEBUG("reclassifying input raster using match array: %s", m)
                focal.array= binary_reclassify(
                    raster=focal,
                    match=_MATCH_ARRAYS[m]
                )
            for window in _WINDOW_DIMS:
                filename=str(_OUTFILE_NAME+"_"+str(window)+"x"+str(window))
                logger.DEBUG("applying moving_windows.filter to reclassed "
                "array for window size: %s", window)
                filter(
                    r = focal,
                    function = _FUNCTION,
                    size = window,
                    dest_file = filename
                )
                if not logger.disabled : progress.update()

    # otherwise just do our ndimage filtering
    else:
        for window in _WINDOW_DIMS:
            filename = str(_OUTFILE_NAME+"_"+str(window)+"x"+str(window))
            test = filter(
                r = r,
                function = _FUNCTION,
                size = window,
                dest_filename = filename)
            if not logger.disabled : progress.update()
