#!/usr/bin/env python3

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2018, Playa Lakes Joint Venture"
__credits__ = ["Kyle Taylor", "Alex Daniels", "Meghan Bogaerts", "Stephen Chang"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"


import os
import fiona
import geopandas as gp
import pandas as pd
import json
import magic
import psycopg2

import pyproj

from shapely.geometry import shape

from .do import Local, EE, Do, _build_kwargs_from_args

import logging

_DEFAULT_EPSG = 4326

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# # Fickle beast handlers for Earth Engine
try:
    import ee
    ee.Initialize()
except Exception:
    logger.warning("Failed to load the Earth Engine API. "
                   "Check your installation. Will continue "
                   "to load but without the EE functionality.")

def rebuild_crs(*args):
    """
    Build a CRS dict for a user-specified Vector or GeoDataFrame object
    :param args:
    :return:
    """
    if isinstance(args[0], "EE"):
        return Do(
            args[2:],
            this=_ee_rebuild_crs,
            that=args[1],
        ).run()
    elif isinstance(args[0], "Local"):
        return Do(
            args[2:],
            this=_local_rebuild_crs,
            that=args[1]
        ).run()
    else:
        # our default action is to just assume local operation
        return _local_rebuild_crs(*args)


def _local_rebuild_crs(*args):
    _gdf = args[0]
    _gdf.crs = fiona.crs.from_epsg(int(_gdf.crs['init'].split(":")[1]))
    return _gdf


def _ee_rebuild_crs(*args):
    pass


def _geom_units(*args):
    try:
        _gdf = args[0]
    except IndexError:
        raise IndexError("1st positional argument should either "
                         "be a Vector or GeoDataFrame object")
    if isinstance(_gdf, Vector):
        _gdf = _gdf.to_geodataframe()
    # by default, there should be a units key
    # associated with the CRS dict object. Prefer
    # to use that units entry
    try:
        return _gdf['crs']['units']
    # otherwise, let's hackishly lean on pyproj to figure out
    # units from the full PROJ.4 string
    except KeyError:
        proj_4_string = pyproj.Proj(
            "+init=EPSG:"+str(_gdf.crs['init'].split(":")[1])
        )
        _units = proj_4_string.srs.split("+units=")[1].split(" +")[0]
        if _units.find("m") != -1:
            return "m"
        else:
            return _units


class Geometries(object):
    """
    Cast a list of features as a shapely geometries. 
    :param geometries:
    :return: None
    """
    def __init__(self, *args, **kwargs):
        
        self._geometries = []

        # argument handlers
        KNOWN_ARGS = ['geometries']
        DEFAULTS = [None]
        if len(args) > 0:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(kwargs, defaults=DEFAULTS, keys=KNOWN_ARGS)
        if kwargs['geometries']:
            self.geometries = kwargs['geometries']
        else:
            pass # allow empty specification
    
    @property
    def geometries(self):
        return self._geometries

    @geometries.setter
    def geometries(self, *args):
        try:
            self._geometries = [shape(ft['geometry']) for ft in list(args[0])]
        except AttributeError:
            logger.debug("Failed to process shapely geometries from input: %s -- trying direct assignment.", args[0])
            self._geometries = args[0]


class EeGeometries(Geometries):
    pass


class Attributes(object):
    def __init__(self, *args, **kwargs):
        
        self._attributes = {}

        # argument handlers
        KNOWN_ARGS = ['shape_collection']
        DEFAULTS = [None]
        if len(args) > 0:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(kwargs, defaults=DEFAULTS, keys=KNOWN_ARGS)
        if kwargs['shape_collection']:
            self.attributes = kwargs['shape_collection']
        else:
            pass # allow empty specification

    @property
    def attributes(self):
        return self._attributes
    
    @attributes.setter
    def attributes(self, *args):
        try:
            self._attributes = pd.DataFrame([ dict(item['properties']) for item in list(args[0]) ])
        except AttributeError:
            logger.debug("Failed to process attribute table as a pandas DataFrame: %s -- trying direct assignment.", args[0])
            self._attributes = args[0]
            
class EeAttributes(Attributes):
    pass

class Vector(object):
    def __init__(self, *args, **kwargs):
        """Builder class that handles file input/output operations for shapefiles using fiona and shapely 
           built-ins and performs select spatial modifications on vector datasets

        Keyword arguments:
        filename= the full path filename to a vector dataset (typically a .shp file).
        json=jsonified text
        Positional arguments:
        1st= if no filname keyword argument was used, attempt to read the first positional\
             argument
        """
        self.geometries = []
        self.attributes = {}
        self.schema = []
        self.crs = []
        self.crs_wkt = []
        # argument handlers
        KNOWN_ARGS = ['input']
        DEFAULTS = [None]
        if len(args) > 0:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(kwargs, defaults=DEFAULTS, keys=KNOWN_ARGS)
        # specification for class methods
        if kwargs['input'] is None:
            # allow an empty specification
            pass
        elif self._is_file(kwargs['input']):
            logger.debug("Accepting user-input as file and attempting read: %s", kwargs['input'])
            _features = self._builder(filename=kwargs['input'])
        elif self._is_json_string(kwargs['input']):
            logger.debug("Accepting user-input as json string and attempting read: %s", kwargs['input'])
            _features =  self._builder(json=kwargs['input'])
        elif isinstance(kwargs['input'], gp):
            logger.debug("Accepting user-input as geopandas and attempting read: %s", kwargs['input'])
            _features =  self._builder(json=kwargs['input'].to_json())
        else:
            logger.exception("Unhandled input provided to Vector()")
            raise ValueError()

        self.attributes, self.geometries = _features.attributes, _features.geometries

    def __copy__(self):
        """ Simple copy method that creates a new instance of a vector class and assigns 
            default attributes from the parent instance
        """
        _vector_geom = Vector()
        _vector_geom.geometries = self.geometries
        _vector_geom.attributes = self.attributes
        _vector_geom.crs = self.crs
        _vector_geom.crs_wkt = self.crs_wkt
        _vector_geom.schema = self.schema
        _vector_geom.filename = self.filename

        return _vector_geom

    def __deepcopy__(self, memodict={}):
        """ copy already is a deep copy """
        return self.__copy__()

    def _is_string(self, string=None):
        try:
            return type(string) == str
        except Exception:
            return False
        return False

    def _is_file(self, string=None):
        try:
            if os.path.exists(string):
                return True
        except Exception:
            return False
        return False
    
    def _is_geojson_file(self, path=None):
        return magic.from_file(path).find('ASCII') >= 0

    def _is_json_string(self, string=None):
        """
        Use json.loads() to test whether this is a valid json string
        """
        try:
            json.loads(string)
            return True
        except Exception:
            return False
        return False

    def _is_shapefile(self, path=None):
        return magic.from_file(path).find('ESRI Shapefile') >= 0

    def _is_geopackage(self, path=None):
        return magic.from_file(path).find('SQLite') >= 0

    def _is_postgis(self, string=None):
        raise NotImplementedError

    def _builder(self, *args, **kwargs):
        """
        Accepts a GeoJSON string or string path to a file that is used to 
        build an appropriate child. The derived class is returned to the user.

        Arguments:
        filename= the full path filename to a vector dataset (typically a .shp file)
        string= json string that we should assign our geometries from
        layer= layer name to use in our SQLite database (if used)
        driver= driver interface to pass to Fiona (default is 'GPKG')

        :return: An appropriate derived vector object
        """
        # argument handlers
        KNOWN_ARGS = ['filename','json','layer','driver']
        DEFAULTS = [None,None,None,'GPKG']
        if len(args) > 0:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(kwargs, defaults=DEFAULTS, keys=KNOWN_ARGS)
        # args[0] / -filename
        if self._is_file(kwargs['filename']):
            if self._is_geojson_file(kwargs['filename']):
                return GeoJson(filename=kwargs['filename'])
            elif self._is_shapefile(kwargs['filename']):
                return Shapefile(kwargs['filename'])
            elif self._is_geopackage(kwargs['filename']):
                return GeoPackage(kwargs['filename'], kwargs['layer'], kwargs['driver'])
            elif self._is_postgis(kwargs['filename']):
                return PostGis(kwargs['filename'], kwargs['dsn'])
            else:
                raise FileNotFoundError("Couldn't process the provided filename as vector data")
        if self._is_json_string(kwargs['json']):
            return GeoJson(json_string=kwargs['json'])
        else:
            raise ValueError("Couldn't handle input data provided by user -- is this a valid JSON string or filename?")

    def to_geometries(self, geometries=None):
        """
        Cast a list of features as a shapely geometries. 
        :param geometries:
        :return: None
        """
        if geometries:
            return Geometries(geometries).geometries
        else:
            return Geometries(self.geometries).geometries

    def to_geodataframe(self):
        """ return our spatial data as a geopandas dataframe """
        try:
            _gdf = gp.GeoDataFrame({
                "geometry": gp.GeoSeries(self.geometries),
            })
            _gdf.crs = self.crs
            # merge in our attributes
            _gdf = _gdf.join(self.attributes)
        except Exception:
            logger.debug("failed to build a GeoDataFrame from shapely"
                           "geometries -- will try to read from original"
                           " source file instead")
            _gdf = gp.read_file(self.filename)
        return _gdf

    def to_geopandas(self):
        """
        Shorthand to to_geodataframe()
        :return:
        """
        return self.to_geodataframe()

    def to_geojson(self, stringify=None):
        return GeoJson()

    def to_ee_feature_collection(self):
        return ee.FeatureCollection(self.to_geojson(stringify=True))


class Fiona(object):
    def __init__(self, *args, **kwargs):
        self.filename = []
        # argument handlers
        KNOWN_ARGS = ['input', 'layer', 'driver']
        DEFAULTS = [None, None, 'ESRI Shapefile']
        if args[0]:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(args)
        # by default, process this as a file and parse out or data using Fiona
        if kwargs['input'] is not None:
            self.filename = kwargs['input']
            _shape_collection = fiona.open(kwargs['input'], layer=kwargs['layer'], driver=kwargs['driver'])
        else:
            raise ValueError("input= argument cannot be 'None'")
        # assign our class private members from whatever was read
        # from input
        self.crs = _shape_collection.crs
        self.crs_wkt = _shape_collection.crs_wkt
        self.schema = _shape_collection.schema
        # parse our dict of geometries into an actual shapely list
        self.geometries = Geometries(_shape_collection).geometries
        self.attributes = Attributes(_shape_collection).attributes

    def write(self, filename=None, type='ESRI Shapefile'):
        """ wrapper for fiona.open that will write in-class geometry data to disk

        (Optional) Keyword arguments:
        filename -- the full path filename to a vector dataset (typically a .shp file)
        (Optional) Positional arguments:
        1st -- if no keyword argument was used, attempt to .read the first pos argument
        """
        # args[0] / filename=
        if filename is not None:
            self.filename = filename
        # call fiona to write our geometry to disk
        with fiona.open(
            self.filename,
            'w',
            type,
            crs=self.crs,
            schema=self.schema
        ) as shape:
            shape.write({
                'geometry': mapping(self.geometries),
                'properties': self.attributes.to_dict(),
            })


class Shapefile(Fiona):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class GeoJson(Fiona):
    def __init__(self, *args, **kwargs):
        """
        :param args:
        :return:
        """
        # argument handlers
        KNOWN_ARGS = ['filename', 'json', 'stringify']
        DEFAULTS = [None, None, True]
        if args[0]:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(args)
        # build a target dictionary
        feature_collection = {
            "type": "FeatureCollection",
            "features": [],
            "crs": [],
            "properties": []
        }
        if kwargs['filename']:
            super().__init__(input=kwargs['filename'])
        elif kwargs['json']:
            super().__init__()
            self.geometries = self._to_geometries(kwargs['json'])
            logger.debug("dropping attribute table from input json object -- not implemented yet?")
        else:
            logger.debug('Unknown input passed to GeoJson constructor by user')
            raise ValueError
        # iterate over features in our shapely geometries
        # and build-out our feature_collection
        for feature in self.geometries:
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
        if self.crs:
            feature_collection["crs"].append(self.crs)
        # define our properties (attributes)
        for i in self.attributes.index:
            feature_collection['properties'].append(
                self.attributes.loc[i].to_json()
            )
        # do we want this stringified?
        if kwargs['stringify']:
            feature_collection = json.dumps(feature_collection)

    def _to_geometries(self, string=None):
        """
        Accepts a json string and parses it into a shapely feature collection
        :param string: GeoJSON string containing a feature collection to parse
        :return: None
        """
        _json = json.loads(string)
        try:
            _type = _json['type']
            _features = _json['features']
        except KeyError:
            raise KeyError("Unable to parse features from json. "
                           "Is this not a GeoJSON string?")
        try:
            self.crs = _json['crs']
        except KeyError:
            # nobody uses CRS with GeoJSON, but it's default
            # projection is typically EPSG:4326
            logger.warning("no crs property defined for json input "
                           "-- assuming EPSG:"+_DEFAULT_EPSG)
            self.crs = {'crs': 'epsg:'+_DEFAULT_EPSG}
        # listcomp : iterate over our features and convert them
        # to shape geometries
        return [shape(ft['geometry']) for ft in _features]

    def read(self):
        """Read JSON data from a file using fiona"""
        raise NotImplementedError

class GeoPackage(Fiona):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        
class PostGis(object):
    def __init__(self, *args, **kwargs):
        pass


class FeatureCollection(EeGeometries, EeAttributes):
    def __init__(self, *args, **kwargs):
        super().__init__()

