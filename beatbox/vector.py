
#!/usr/bin/env python2

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2018, Playa Lakes Joint Venture"
__credits__ = ["Kyle Taylor", "Alex Daniels", "Meghan Bogaerts", "Stephen Chang"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"


import sys
import os
import fiona
import geopandas as gp
import json

from copy import copy
from shapely.geometry import *

import logging

_DEFAULT_EPSG = 2163

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fickle beast handlers for Earth Engine
try:
    import ee
    ee.Initialize()
except Exception:
    logger.warning("Failed to load the Earth Engine API. "
                   "Will continue to load but without "
                   "the EE functionality.")


class Vector(object):
    def __init__(self, *args, **kwargs):
        """Handles file input/output operations for shapefiles \
        using fiona and shapely built-ins and performs select \
        spatial modifications on vector datasets

        Keyword arguments:
        filename= the full path filename to a vector
        dataset (typically a .shp file)
        json=jsonified text
        Positional arguments:
        1st= if no filname keyword argument was used,
        attempt to read the first positional argument
        """
        self._geometries = []
        self._attributes = {}
        self._filename = []
        self._schema = []
        self._crs = []
        self._crs_wkt = []
        # args[0] / filename= / json=
        try:
            # assume it's a filename
            if os.path.exists(args[0]):
                self.filename = args[0]
            # if our read fails, check
            # to see if it's JSON
            elif is_json(args[0]):
                self.read(string=args[0])
        except IndexError:
            if kwargs.get('filename'):
                self.filename = kwargs.get('filename')
            elif kwargs.get('json'):
                self.read(string=kwargs.get('json'))
            pass # allow empty specification
        except Exception as e:
            raise e
        # if the user specified a filename, try to open it
        if self.filename:
            self.read(self.filename)

    def __copy__(self):
        """ simple copy method that creates a new instance of a vector class and assigns \
        default attributes from the parent instance
        """
        _vector_geom = Vector()
        _vector_geom._geometries = self._geometries
        _vector_geom._attributes = self._attributes
        _vector_geom._crs = self._crs
        _vector_geom._crs_wkt = self._crs_wkt
        _vector_geom._schema = self._schema
        _vector_geom._filename = self._filename

        return _vector_geom

    def __deepcopy__(self, memodict={}):
        """ copy already is a deep copy """
        return self.__copy__()

    @property
    def filename(self):
        """ decorated getter for our filename """
        return self._filename

    @filename.setter
    def filename(self, *args):
        """ decorated setter for our filename """
        try:
            self._filename = args[0]
        except Exception:
            raise Exception("General error occured while trying to assign filename")

    @property
    def crs(self):
        """ decorated getter for our Coordinate Reference System """
        return self._crs

    @crs.setter
    def crs(self, *args):
        """ decorated setter for our Coordinate Reference System

        Keyword arguments: None

        Positional arguments:
        1st = first positional argument is used to assign our class CRS value
        """
        try:
            self._crs = args[0]
        except Exception:
            raise Exception("General error occurred while trying to assign CRS")

    @property
    def schema(self):
        """ decorated getter for our schema """
        return(self._schema)

    @schema.setter
    def schema(self, *args):
        """ decorated setter for our schema

        Keyword arguments: None

        Positional arguments:
        1st = first positional argument is used to assign our class schema value
        """
        try:
            self._schema = args[0]
        except Exception:
            logger.warning("invalid schema argument provided -- falling back on "
                           "default from our shapely geometries")

    @property
    def geometries(self):
        """ decorated getter for our fiona geometries collection """
        return self._geometries

    @geometries.setter
    def geometries(self, *args):
        try:
            self._geometries = args[0]
        except Exception as e:
            raise e
        # default behavior is to accept shapely geometry as input, but a
        # user may pass a string path to a shapefile and we will handle the input
        # and write it to output -- this is essentially a file copy operation
        try:
            self.read(self._geometries)
        # did you pass an incorrect filename?
        except OSError:
            raise OSError("Unable to read file passed passed by user")
        # it can't be a string path -- assume is a Collection or Geometry object
        # and pass it on
        except Exception:
            self._fiona_to_shapely_geometries(geometries=self._geometries)

    def _fiona_to_shapely_geometries(self, geometries=None):
        """
        Cast a list of features as a shapely geometries. This is
        used to assign internal geometries from fiona -> shapely geometries
        :param geometries:
        :return: None
        """
        self._geometries = [shape(ft['geometry']) for ft in list(geometries)]

    def _json_string_to_shapely_geometries(self, string=None):
        """
        Accepts a json string and parses it into a shapely feature collection
        stored internally
        :param string: GeoJSON string containing a feature collection to parse
        :return: None
        """
        # determine if string= is even json
        try:
            _json = json.loads(string)
        except json.JSONDecodeError:
            raise Exception("unable to process string= "
                            "argument... is this not a json string?")
        # determine if string= is geojson
        try:
            _type = _json['type']
            _features = _json['features']
        except KeyError:
            raise KeyError("Unable to parse features from json. "
                           "Is this not a GeoJSON string?")
        try:
            self._crs = _json['crs']
        except KeyError:
            # nobody uses CRS with GeoJSON -- but it's default
            # projection is always(?) EPSG:4326
            logger.warning("no crs property defined for json input "
                           "-- assuming EPSG:4326")
            self._crs = {'crs': 'epsg:4326'}
        # listcomp : iterate over our features and convert them
        # to shape geometries
        self._geometries = [shape(ft['geometry']) for ft in _features]

    def read(self, *args, **kwargs):
        """
        Accepts a GeoJSON string or string path to a shapefile that is read
        and used to assign internal class variables for CRS, geometries, and schema

        Keyword arguments:
        filename= the full path filename to a vector dataset (typically a .shp file)
        string= json string that we should assign our geometries from

        Positional arguments:
        1st = either a full path to a file or a geojson string object
        :return: None
        """
        # sandbox for potential JSON data
        _json = None
        # args[0] / -filename / -string
        try:
            if os.path.exists(args[0]):
                self._filename = args[0]
            elif is_json(args[0]):
                    _json = args[0]
            else:
                raise AttributeError("Unable to process first positional argument as a file or geojson string")
        except IndexError:
            if kwargs.get('filename'):
                if os.path.exists(kwargs.get('filename')):
                    self._filename = kwargs.get('filename')
                else:
                    raise FileNotFoundError("invalid filename= argument supplied by user")
            elif kwargs.get('string'):
                if is_json(kwargs.get('string')):
                    _json = kwargs.get('string')
            # allow no arguments exception -- that we explicitly set
            # filename elsewhere and we are calling .read() without
            # arguments
            pass
        # if this is a json string, parse out our geometry and attribute
        # data accordingly
        if _json:
            self._json_string_to_shapely_geometries(string=_json)
        # otherwise, assume this is a file and parse out or data using Fiona
        else:
            _shape_collection = fiona.open(self.filename)
            try:
                self._crs = _shape_collection.crs
                self._crs_wkt = _shape_collection.crs_wkt
                # parse our dict of geometries into an actual shapely list
                self._fiona_to_shapely_geometries(geometries=_shape_collection)
                self._schema = _shape_collection.schema
            except Exception as e:
                raise e

    def write(self, *args, **kwargs):
        """ wrapper for fiona.open that will write in-class geometry data to disk

        (Optional) Keyword arguments:
        filename -- the full path filename to a vector dataset (typically a .shp file)
        (Optional) Positional arguments:
        1st -- if no keyword argument was used, attempt to .read the first pos argument
        """
        # args[0] / filename=
        try:
            self.filename = args[0]
        except IndexError:
            if kwargs.get('filename', False):
                self.filename = kwargs.get('filename')
            # perhaps we explicitly set our filename elsewhere
            pass
        # args[1] / type=
        _type = 'ESRI Shapefile' # by default, write as a shapefile
        try:
            _type = args[1]
        except IndexError:
            if kwargs.get('type', False):
                _type = kwargs.get('type')
            pass
        try:
            # call fiona to write our geometry to disk
            with fiona.open(
                self.filename,
                'w',
                _type,
                crs=self.crs,
                schema=self.schema
            ) as shape:
                # If there are multiple geometries, put the "for" loop here
                shape.write({
                    'geometry': mapping(self.geometries),
                    'properties': {'id': 123},
                })
        except Exception:
            raise Exception("General error encountered trying "
                            "to call fiona.open on the input data. "
                            "Is the file not a shapefile?")

    def to_shapely_collection(self):
        """ return a shapely collection of our geometry data """
        return self.geometries

    def to_geodataframe(self):
        """ return our spatial data as a geopandas dataframe """
        try:
            _gdf = gp.GeoDataFrame({
                "geometry": gp.GeoSeries(self._geometries),
            })
            _gdf.crs = self._crs
        except Exception:
            logger.warning("failed to build a GeoDataFrame from shapely geometries -- "
                           "will try to read from original source file instead")
            _gdf = gp.read_file(self._filename)
        # make sure we note our units, because GeoPandas doesn't by default
        #_gdf.crs["units"] = "degrees" if _gdf.geometry[0].wkt.find('.') != -1 else "meters"
        return _gdf

    def to_ee_feature_collection(self):
        return ee.FeatureCollection(self.to_geojson(stringify=True))

    def to_geojson(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """
        _as_string = False
        try:
            _as_string = True if kwargs.get("stringify", args) else False
        except IndexError:
            _as_string = False
        except Exception as e:
            raise e
        # build a target dictionary
        feature_collection = {
            "type": "FeatureCollection",
            "features": [],
            "crs": []
        }
        # iterate over features in our shapely geometries
        # and build-out our feature_collection
        for feature in self._geometries:
            if isinstance(feature, dict):
                feature_collection["features"].append(feature)
            else:
                # assume that json will know what to do with it
                # and raise an error if it doesn't
                try:
                    feature_collection["features"].append(json.loads(feature))
                except Exception as e:
                    raise e
        # note the CRS
        if self._crs:
            feature_collection["crs"].append(self._crs)
        # do we want this stringified?
        if _as_string:
            feature_collection = json.dumps(feature_collection)

        return feature_collection

def rebuild_crs(vector=None):
    """
    
    """
    vector.crs = fiona.crs.from_epsg(int(self.crs['init'].split(":")[1]))
    return vector

def is_json(*args, **kwargs):
    try:
        _string = args[0]
    except IndexError:
        _string = kwargs.get("string")
        if _string is None:
            raise IndexError("invalid string= argument passed by user")
    # sneakily use json.loads() to test whether this is
    # a valid json string
    try:
        _string = json.loads(_string)
        return True
    except json.JSONDecodeError:
        return False
    except Exception:
        raise Exception("General exception caught trying to parse string="
                        " input. This shouldn't happen.")
    return False

