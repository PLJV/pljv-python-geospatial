#!/usr/bin/env python3

__author__ = "Kyle Taylor"
__copyright__ = "Copyright 2017, Playa Lakes Joint Venture"
__credits__ = ["Kyle Taylor"]
__license__ = "GPL"
__version__ = "3"
__maintainer__ = "Kyle Taylor"
__email__ = "kyle.taylor@pljv.org"
__status__ = "Testing"

import logging

logger = logging.getLogger(__name__)


class Backend(object):
    """
    Default backend interface
    """

    _backend_code = {"local": 0, "ee": 1}
    _what = None
    _with = None


class Local(Backend):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError


class EE(Backend):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError


class Do(Backend):
    def __init__(self, this=None, that=None, *args):
        """
        Do is a dictcomp interface for performing arbitrary spatial tasks with
        Vector and Raster objects

        :param function this: run 'this' function
        :param class that: Backend class describing with 'this' function's parameters
        :param list args: list of any additional positional arguments that are passed to the 'this' function
        """
        if this is None or that is None:
            try:
                self._what = args[0]
                self._with = args[1]
                args = args[:2]
            except IndexError:
                raise IndexError(
                    "this=, that= are empty and we failed to ",
                    "parse any positional arguments",
                )
        else:
            self._what = this  # run function
            self._with = that  # Currently EE or Local are supported
        self._using = self._unpack_with_arguments(*args)

    def _unpack_with_arguments(self, *args, **kwargs):
        """
        The what arguments specified by the user can be pass as a dictionary or as a list. This
        method will unpack user-specified 'with' arguments so that they can be handled by a user-specified
        'what' function
        :return: None
        """
        return args

    @property
    def run(self):
        """
        Get method that will call our user-supplied run function
        :param args:
        :return: Result of a what function
        """
        # if we haven't already specified our 'what' and 'with' parameters
        if self._what is None or self._with is None:
            raise AttributeError("'what' and 'with' parameters are undefined.")
        return self._what(self._with)

    @run.setter
    def run(self, *args):
        """
        Set method for our run function. This specifies what we are going to do with our instance
        and does some object checks to determine an appropriate backend based on objects specified
        with the 'what' parameter. The setter will then call the function to perform the user
        specified action
        :param args:
        :return:
        """
        try:
            self._what = args[0]["what"]
            self._with = args[0]["with"]
        except KeyError as e:
            logger.exception(
                "run= accepts a dict as a single positional argument specifying "
                "'what' and 'with' keys : %s",
                e,
            )
            raise KeyError()
        self._unpack_with_arguments()
