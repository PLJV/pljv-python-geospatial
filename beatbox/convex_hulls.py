#!/usr/bin/env python2

__author__ = ["Kyle Taylor", "Stephen Chang"]
__copyright__ = "Copyright 2017, Playa Lakes Joint Venture"
__credits__ = ["Stephen Chang", "Kyle Taylor"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

import logging
import numpy as np
import geopandas as gp
import pandas as pd
import fiona

from beatbox.vector import Vector, _local_rebuild_crs
from beatbox.do import Backend, EE, Local, Do

from copy import copy
from scipy.sparse.csgraph import connected_components

_DEFAULT_EPSG = 2163
_DEFAULT_BUFFER_WIDTH = (
    1000
)  # default width (in meters) of a geometry for various buffer operations
_ARRAY_MAX = 2e6  # maximum array length to attempt numpy operations on before chunking

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _split_in_chunks(*args):
    """
    Hidden function that will accept an array (list) and split it up into
    chunks of an arbitrary length using the Python yield built-in
    :param arg1: A list of things we wish to split into chunks
    :return: Generator object
    """
    try:
        _array = args[0]
        _n_chunks = args[1]
    except IndexError as e:
        raise e
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(_array), _n_chunks):
        yield _array[i : i + _n_chunks]


def _dissolve_overlapping_geometries(buffers=None):
    """
    Hidden function that will accept a GeoDataFrame containing Polygons,
    explode the geometries from single part to multi part, and then dissolve
    geometries that overlap spatially. Implementation is robust for extremely
    large GeoDataFrames. Proposed dropping this in the future. A better implementation
    is already available upstream in GeoPandas
    :param arg1: A GeoDataFrame or Vector object specifying source buffers 'groups' we intend to attribute with
    :param buffers: Keyword specification for first positional argument
    :return: GeoDataFrame
    """
    # args[0] / buffers=
    if buffers is None:
        raise IndexError("invalid buffers= " "argument provided by user")
    # force casting as a GeoDataFrame
    try:
        _crs = buffers.crs  # retain our original CRS
        buffers = gp.GeoDataFrame({"geometry": buffers})
    except ValueError:
        if isinstance(buffers, gp.GeoDataFrame):
            pass
        else:
            raise ValueError(
                "Invalid buffers= argument input -- failed to"
                " make a GeoDataFrame from input provided"
            )
    except Exception:
        raise Exception(
            "Unable to cast buffers= argument (type:"
            + str(type(buffers))
            + ") as a GeoDataFrame."
        )
    # determine appropriate groupings for our overlapping buffers -- implementation may differ based
    # on how large the original GeoDataFrame is
    if buffers.size > _ARRAY_MAX:
        split = int(round(buffers.size / _ARRAY_MAX) + 1)
        logger.warning(
            "Attempting intersect operation on a large "
            "vector dataset -- processing in %s chunks, "
            "which may lead to artifacts at boundaries. "
            "ETA: %s min",
            split,
            round((split * 16.5 ** 2) / 60),
        )
        chunks = _split_in_chunks(buffers, split)
        try:
            # listcomp magic : for each geometry, determine whether it overlaps with
            # all other geometries in this chunk
            overlap_matrix = np.concatenate(
                [
                    buffers.geometry.overlaps(d).values.astype(int)
                    for i, d in enumerate(chunks)
                ]
            )
            # free-up our RAM
            del chunks
        except AttributeError as e:
            raise AttributeError(
                "Encountered an error when checking for overlaps in chunks of buffered input. "
                "This shouldn't happen. Consider updating your libraries with conda/pip and try"
                " again. Full attribute error: " + str(e)
            )
        # merge attributes
        overlap_matrix.shape = (buffers.size, buffers.size)
        n, ids = connected_components(overlap_matrix)
        dissolved_buffers = gp.GeoDataFrame(
            {"geometry": buffers.geometry, "group": ids}
        )
        # call geopandas dissolve with our 'ids' column and
        dissolved_buffers = dissolved_buffers.dissolve(by="group")
        dissolved_buffers.crs = _crs
    else:
        # a sane default implementation used for most small GeoDataFrames
        # this is mostly redundant -- a user can call unary_union directly
        dissolved_buffers = buffers.geometry.unary_union
        dissolved_buffers = gp.GeoDataFrame(
            {
                "geometry": dissolved_buffers,
                "group": pd.Series(range(1, len(dissolved_buffers) + 1)),
            }
        )
        dissolved_buffers.crs = _crs
    return dissolved_buffers


def _spatial_join(buffers=None, points=None):
    """
    Hidden function that will use the group attribute from intersecting polygon features to classify
    point geometries. This is intended to be used as a fuzzy classifier and is a shorthand for gp.sjoin().
    :param arg1: A GeoDataFrame or Vector object specifying source buffers 'groups' we intend to attribute with
    :param arg2: A GeoDataFrame or Vector object specifying source points to be attributed
    :param buffers: Keyword specification for first positional argument
    :param points: Keyword specification for second positional argument
    :return: GeoDataFrame
    """
    # args[0] / buffers=
    if buffers is None:
        raise IndexError("invalid buffers= argument provided by user")
    # args[1] / points=
    if points is None:
        raise IndexError("invalid points= argument provided by user")
    # explode and dissolve geometries
    gdf_out = _dissolve_overlapping_geometries(buffers)
    # ensure consistent CRS
    gdf_out = gdf_out.to_crs(points.crs)
    # return the right-sided spatial join
    return gp.sjoin(points, gdf_out, how="inner", op="intersects").rename(
        columns={"index_right": "clst_id"}
    )


def _local_convex_hull(points=None):
    """
    Accepts point features as a GeoDataFrame and uses geopandas to
    calculate a convex hull from the geometries
    :param arg1: A GeoDataFrame or Vector object specifying source points we intend to buffer
    :param points: A keyword specification for our first positional argument
    :return: GeoDataFrame
    """
    # args[0]/points=
    if points is None:
        raise IndexError("invalid points= argument provided by user")
    # try and process our lone 'points' argument
    if isinstance(points, gp.GeoDataFrame):
        pass
    elif isinstance(points, Vector):
        points = points.to_geodataframe()
    else:
        raise AttributeError(
            "points= input is invalid. try passing", " a GeoDataFrame or Vector object."
        )
    # GeoPandasDataframe->convex_hull()
    return points.convex_hull()


def _ee_fuzzy_convex_hull(points=None, width=_DEFAULT_BUFFER_WIDTH):
    raise NotImplementedError


def _local_fuzzy_convex_hull(points=None, width=_DEFAULT_BUFFER_WIDTH):
    """
    Accepts a GeoDataFrame containing points, buffers the point geometries by some distance,
    and than builds convex hulls from point clusters
    :param arg1: A GeoDataFrame or Vector object specifying source points we intend to buffer
    :param arg2: An integer value (in meters) specifying the radius we wish to buffer point features by
    :param points: Keyword argument for arg1
    :param width: Keyword argument for arg2
    :return: GeoDataFrame
    """
    # args[0] / points=
    if points is None:
        raise IndexError("invalid points= argument passed by user")
    # cast our points features as a gdf (if they aren't already)
    if isinstance(points, str):
        points = Vector(points).to_geodataframe()
    # reproject to something that uses metric units
    points = _local_rebuild_crs(points)
    points = points.to_crs(epsg=_DEFAULT_EPSG)
    # generate circular point buffers around our SpatialPoints features
    try:
        point_buffers = copy(points)
        # adjust the width= parameter based on the projection
        # of our point buffers
        point_buffers = point_buffers.buffer(width).unary_union
        point_buffers = gp.GeoDataFrame(
            {
                "geometry": point_buffers,
                "group": pd.Series(range(1, len(point_buffers) + 1)),
            }
        )
        # set the CRS
        point_buffers.crs = {"init": "epsg:" + str(_DEFAULT_EPSG)}
    except Exception as e:
        raise e
    # spatial join of our point_buffers
    point_clusters = _spatial_join(point_buffers, points)
    # drop any extra columns lurking in our point clusters data
    # and dissolve by our clst_id field
    if len(point_clusters.columns) > 2:
        cols_to_remove = list(point_clusters.columns)
        # i.e., not our clst_id or geometries
        cols_to_remove.remove("clst_id")
        cols_to_remove.remove("geometry")
        for col in cols_to_remove:
            del point_clusters[col]
    # group our point clusters by cluster id
    _clst_id = point_clusters["clst_id"]
    point_clusters = point_clusters.dissolve(by="clst_id")
    point_clusters["clst_id"] = _clst_id
    # estimate our convex hulls and drop geometries that are not polygons
    convex_hulls = point_clusters.convex_hull
    convex_hulls = convex_hulls[
        [str(ft).find("POLYGON") != -1 for ft in convex_hulls.geometry]
    ]
    # return our convex hulls as a GeoDataFrame
    gdf = gp.GeoDataFrame({"geometry": convex_hulls})
    gdf["clst_id"] = pd.Series(range(0, gdf.shape[0]))
    gdf.crs = points.crs
    # sanity check
    if len(gdf) < 1:
        logger.warning(
            "Length of our convex hulls generated from buffered "
            "point features is <1, this shouldn't happen."
        )
    return gdf


def _guess_backend(obj=None):
    """
    Will attempt to parse a proper backend code based on object context
    """
    if obj is None:
        return None
    elif isinstance(obj, "Raster") or isinstance(obj, "Vector"):
        return obj.backend
    elif isinstance(obj, "GeoRaster") or isinstance(obj, "GeoDataFrame"):
        return Backend._backend_code["local"]
    else:
        return "unknown"


def fuzzy_convex_hull(obj=None, width=_DEFAULT_BUFFER_WIDTH):
    """
    Fuzzy convex hull wrapper function that will call either a local or earth engine
    implementation of the Carter fuzzy convex hull generator. Currently only a local
    version of this is implemented.
    :param arg1: A GeoDataFrame or Vector object specifying source points we intend to buffer
    :param arg2: An integer value (in meters) specifying the radius we wish to buffer point features by
    :param points: Keyword specification for first positional arg
    :param width: Keyword specification for second positional arg
    :return: GeoDataFrame
    """
    # args[0]/points=
    if obj is None:
        raise IndexError("invalid points= argument")
    # launch our context runner
    if isinstance(obj, EE):
        return Do(this=_ee_fuzzy_convex_hull, that=[obj, width]).run()
    elif isinstance(obj, Local):
        return Do(this=_local_fuzzy_convex_hull, that=[obj, width]).run()
    else:
        # our default action is to just assume local operation
        return _local_fuzzy_convex_hull(points=obj, width=width)
