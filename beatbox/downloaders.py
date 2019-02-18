#!/usr/bin/env python3

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2017, Playa Lakes Joint Venture"
__credits__ = ["Kyle Taylor"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

import warnings
warnings.filterwarnings("ignore")

import os
import re
import requests
import urllib
import logging
import ntpath

from bs4 import BeautifulSoup as bs

_CDL_BASE_URL = "http://www.nass.usda.gov/Research_and_Science/Cropland/" \
                "Release/"
_PROBABLE_PLAYAS_BASE_URL = "https://pljv.org/for-habitat-partners/maps" \
                                 "-and-data/maps-of-probable-playas/"
_FAA_DOF_URL = "https://www.faa.gov/air_traffic/flight_info/aeronav/digi" \
               "tal_products/dof/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HttpDownload(object):
    def __init__(self, *args, **kwargs):
        """
        Default web scraping interface for BeautifulSoup that will scrape
        a user-specified URL for A HREF tags and then download files that match
        some filename pattern using regular expressions (e.g., [.]zip).
        :param args:
        :param kwargs:
        """
        self._re_pattern = None
        self._url = None
        self._html = None
        self._soup = None
        self._files = []
        # args[0] / url=
        try:
            self.url = args[0]
        except IndexError:
            if kwargs.get("url"):
                self.url = kwargs.get("url")
            # allow instantiation without specifying a url
            pass
        # args[1] / pattern=
        try:
            self._re_pattern = args[1]
        except IndexError:
            if kwargs.get("pattern", False):
                self._re_pattern = kwargs.get("pattern")
            # allow instantiation without specifying an re search filter
            pass
        # if we have URL data to work with, validate it
        # to make sure scrape() has something to work with
        if self.url:
            if not self._validate_url():
                raise ValueError("Could not identify any target files at "
                                 "the URL provided using your search "
                                 "pattern.")

    def _validate_url(self, *args, **kwargs):
        """
        Fetch a BeautifulSoup object passed by the user and then check the
        soup for a regular expression search string
        :param args:
        :param kwargs:
        :return: True on found, False on not found
        """
        _pattern = self._re_pattern
        # args[0] / pattern =
        # if the user passed a filename pattern argument, use it,
        # otherwise just use the default pattern specified
        # by _re_pattern
        try:
            self._re_pattern = _pattern = args[0]
        except IndexError:
            if kwargs.get('pattern'):
                self._re_pattern = _pattern = kwargs.get('pattern')
            # allow appending nothing to our search string
            pass
        # dump HTTP server response as xml text
        self._soup = bs(self._html.text, "lxml")
        # iterate over each row looking for an href matching our
        # re search pattern
        for i, a in enumerate(self._soup.findAll("a")):
            if re.search(string=str(a), pattern="href.*." + _pattern):
                return True
        # default action if we didn't find our re search string
        return False

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, *args):
        try:
            self._url = args[0]
        except IndexError:
            raise IndexError("url assignment requires a single str argument")
        self._html = requests.get(self.url, verify=False)

    @property
    def files(self):
        return self._files

    @files.setter
    def files(self, *args):
        self._files = args[0]

    def scrape(self, *args, **kwargs):
        """
        :param args[0]: Regular Expression search string to look for in our
        HTTP server response. Note that this is in addition to our trailing
        search string already provided during initialization.
        :param search_str: alternative specification for args[0] (above)
        """
        # if the user provided an extra RE search string to use,
        # then append it to our terminal pattern. Otherwise, just
        # use the terminal pattern
        self._files = [] # reset our files list
        if kwargs.get('search_str', False):
            _re_search_str = kwargs.get('search_str') + ".*." + self._re_pattern
        else:
            try:
                _re_search_str = args[0] + ".*." + self._re_pattern
            except IndexError:
                _re_search_str = self._re_pattern
        # iterate over our soup and store matching href's
        # in the files list
        for i, a in enumerate(self._soup.findAll("a")):
            if re.search(string=str(a), pattern=_re_search_str):
                self._files.append(
                    # by default, use the filename specified by our a hrefs
                    str(self._soup.select("a")[i].attrs['href'])
                )
        if self._files is None:
            raise ValueError("could not parse any target files from the URL "
                             "provided: check the search_str argument")

    def download(self):
        if not self.files:
            self.scrape()
        for i, f in enumerate(self.files):
            self.files[i] = self.files[i].split("/")[-1]
            if not os.path.exists(self.files[i]):
                urllib.urlretrieve(f, self.files[i])
        # return our list of retrieved filenames to the user
        return self.files


class Nass(HttpDownload):
    def __init__(self, *args, **kwargs):
        super(Nass, self).__init__(url=_CDL_BASE_URL, pattern="zip")


class ProbablePlayas(HttpDownload):
    def __init__(self, *args, **kwargs):
        super(ProbablePlayas, self).__init__(url=_PROBABLE_PLAYAS_BASE_URL, pattern="zip")


class FaaWindTurbines(HttpDownload):
    def __init__(self, *args, **kwargs):
        super(FaaWindTurbines, self).__init__(url=_FAA_DOF_URL, pattern="zip")
        # args[0] / date_filter=
        try:
            _date_filter = args[0]
        except IndexError:
            _date_filter = kwargs.get("date_filter")
            if _date_filter is None:
                _date_filter = self.\
                    parse_most_recent_file_from_dof_strings()
            pass
        # scrape using our date filter string
        self.scrape(search_str=_date_filter)

    def parse_most_recent_file_from_dof_strings(self, search_str="DOF"):
        """
        Accepts a list of strings as a single argument. Will parse
        the strings for their date codes and return the most recent
        code as a string literal that we can use for parsing
        :param search_str: second search string used to filter hrefs in our soup
        :return: most recent date stamp as a string
        """
        # There are a number of zip files published at the FAA DOF URL
        # at any time that we can use
        self.scrape(search_str=search_str)
        _date_strings = [file.split("/")[-1] for file in self.files]
        _date_strings = [date_string.split("_")[-1] for date_string in _date_strings]
        _date_strings = [int(date_string.split(".")[:-1][0]) for date_string in _date_strings]
        return str(max(_date_strings))
