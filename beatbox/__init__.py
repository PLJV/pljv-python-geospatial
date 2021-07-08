from .vector import Vector
from .raster import Raster, reproject, get_extent_params, geotransform_to_affine

# from .downloaders import HttpDownload # This is functional, but needs a re-factoring
__all__ = ["Vector", "Raster", "Do"]
