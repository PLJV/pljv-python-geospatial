"""
Setup Script
"""

import os
import platform

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from shutil import copyfile

INSTALL_REQUIRES = [
    'scipy', 'pandas', 'shapely', 'fiona', 'pyproj', 'geopandas',
    'georasters', 'psutil', 'requests', 'bs4', 'gdal', 'numpy',
    'enlighten'
]

LONG_DESCRIPTION = ""

setup(name='beatbox',
      version=0.3,
      description='shell scriptable exposure to ndimage and other tools for big raster processing and spatial analyses',
      license='GPL v.3',
      author=['Kyle Taylor','Alex Daniels','Meghan Mogearts'],
      author_email='kyle.taylor@pljv.org',
      url='http://pljv.org/about',
      long_description=LONG_DESCRIPTION,
      packages=['beatbox'],
      install_requires=INSTALL_REQUIRES,
      scripts=['scripts/gdal_moving_windows.py']
)

