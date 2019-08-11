#!/usr/bin/env python3

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2018, Playa Lakes Joint Venture"
__credits__ = ["Kyle Taylor", "Alex Daniels",
               "Meghan Bogaerts", "Stephen Chang"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

# logging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

import os
import fiona
import geopandas as gp
import pandas as pd
import json
import magic

import psycopg2
import pyproj

from shapely.geometry import shape, mapping
from .network import PostGis

_DEFAULT_EPSG = 4326

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

    def __init__(self, geometries=None):
        if geometries is not None:
            self.geometries = geometries

    @property
    def geometries(self):
        return self._geometries

    @geometries.setter
    def geometries(self, *args):
        try:
            self._geometries = [shape(ft['geometry']) for ft in list(args[0])]
        except AttributeError:
            logger.debug(
                "Failed to process shapely geometries from input: %s -- trying direct assignment.", args[0])
            self._geometries = args[0]


class EeGeometries(Geometries):
    pass


class Attributes(object):
    def __init__(self, shape_collection=None):
        self._attributes = {}
        if shape_collection is not None:
            self.attributes = shape_collection

    @property
    def attributes(self):
        return self._attributes

    @attributes.setter
    def attributes(self, *args):
        """Cast a shapely collection as a pandas dataframe"""
        try:
            self._attributes = pd.DataFrame(
                [dict(item['properties']) for item in list(args[0])])
        except AttributeError:
            logger.debug(
                "Failed to process attribute table as a pandas DataFrame: %s -- trying direct assignment.", args[0])
            self._attributes = args[0]


class EeAttributes(Attributes):
    pass


class Vector(object):
    def __init__(self, input=None, *args):
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

        if not args:
            args = {}
        else:
            args = args[0]

        # specification for class methods
        if input is None:
            # allow an empty specification
            pass
        elif self._is_file(input):
            logger.debug("Accepting user-input as file and attempting read: %s", input)
            self._builder(filename=input)
        elif self._is_json_string(input):
            logger.debug(
                "Accepting user-input as json string and attempting read: %s", input)
            self._builder(json=input)
        elif isinstance(input, gp):
            logger.debug(
                "Accepting user-input as geopandas and attempting read: %s", input)
            self._builder(json=input.to_json())
        else:
            logger.exception("Unhandled input provided to Vector()")
            raise ValueError()

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
            return os.path.exists(string)
        except Exception:
            return False

    def _is_geojson_file(self, path=None):
        logger.debug("GeoJson magic result:" + magic.from_file(path))
        return magic.from_file(path).find('JSON') >= 0

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

    def _builder(self, filename=None, json=None, layer=None, dsn=None, driver='GPKG'):
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

        # args[0] / -filename
        if self._is_file(filename):
            logger.debug("_builder input is a file")
            if self._is_geojson_file(filename):
                logger.debug("_builder input appears to be geojson -- processing")
                _features = GeoJson(filename=filename)
            elif self._is_shapefile(filename):
                logger.debug("_builder input appears to be a shapefile -- processing")
                _features = Shapefile(filename)
            elif self._is_geopackage(filename):
                logger.debug("_builder input appears to be a geopackage -- processing")
                _features = GeoPackage(filename, layer, driver)
            elif self._is_postgis(filename):
                logger.debug("_builder input appears to be a PostGIS database -- processing")
                _features = PostGis(filename, dsn)
            else:
                raise FileNotFoundError(
                    "Couldn't process the provided filename as vector data")
        elif self._is_json_string(json):
            _features = GeoJson(json=json)
        else:
            raise ValueError(
                "Couldn't handle input data provided by user -- is this a valid JSON string or filename?")

        self.attributes = _features.attributes
        self.geometries = _features.geometries
        self.schema = _features.schema
        self.crs = _features.crs
        self.crs_wkt = _features.crs_wkt

    def to_geometries(self, geometries=None):
        """
        Cast a list of features as shapely geometries. 
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
                         "geometries")
            raise Exception()
        return _gdf

    def to_geojson(self, stringify=False):
        """
        Wrapper for GeoDataFrame that will return our Vector geometries
        and attributes formatted as GeoJSON
        """
        if not stringify:
            return json.loads(self.to_geodataframe().to_json())
        return self.to_geodataframe().to_json()

    def to_ee_feature_collection(self):
        raise NotImplementedError
        # return ee.FeatureCollection(self.to_geojson(stringify=True))


class Fiona(object):
    def __init__(self, input=None, layer=None, driver='ESRI Shapefile'):

        self.filename = []
        self.crs = None
        self.crs_wkt = None
        self.schema = None

        if input is not None:
            self.filename = input
            _shape_collection = fiona.open(input, layer=layer, driver=driver)
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

    def write(self, filename=None, type=None):
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
    def __init__(self, **kwargs):
        """
        GeoJson extension for our Fiona interface that is typically used to 
        read geojson files from a hard disk and build a vector object. 
        :param args:
        :return:
        """

        if kwargs.get('filename') is not None:
            super().__init__(input=kwargs.get('filename'), driver='GeoJSON')
        elif kwargs.get('json') is not None:
            super().__init__()
            self.geometries = Geometries(kwargs.get('json')).geometries
            self.attributes = Attributes(self.geometries).attributes
            try:
                self.crs = json.loads(kwargs.get('json'))['crs']['properties']['name']
            except Exception:
                logger.debug('Failed to set CRS from input json string')
        else:
            logger.debug('Unknown input passed to GeoJson constructor by user')
            raise ValueError

    def read(self):
        """Read JSON data from a file using fiona"""
        raise NotImplementedError


class GeoPackage(Fiona):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class FeatureCollection(EeGeometries, EeAttributes):
    def __init__(self, *args, **kwargs):
        super().__init__()
