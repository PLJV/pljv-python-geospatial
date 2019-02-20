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

logging.basicConfig(level=logging.debug)
logger = logging.getLogger(__name__)


class Backend(object):
    """
    Default backend interface
    """
    _backend_code = {'local': 0, 'ee': 1}
    _what = None
    _with = None


class Local(Backend):
    pass


class EE(Backend):
    pass


class Do(Backend):
    def __init__(self, this=None, that=None, *args):
        """
        Do is a dictcomp interface for performing arbitrary spatial tasks with
        Vector and Raster objects
        :param this: run 'this' function
        :param that: Backend class describing with 'this' function's
        parameters
        :param args: list of any additional positional arguments that are
        passed to the 'this' function
        """
        if this is None or that is None:
            try:
                self._what = args[0]
                self._with = args[1]
                args = args[:2]
            except IndexError:
                raise IndexError("this=, that= are empty and we failed to ",
                                 "parse any positional arguments")
        else:
            self._what = this # run function
            self._with = that # Currently EE or Local are supported
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
            self._what = args[0]['what']
            self._with = args[0]['with']
        except KeyError as e:
            logger.exception("run= accepts a dict as a single positional argument specifying "
                             "'what' and 'with' keys : %s", e)
            raise KeyError()
        # determine what backend to use (or if the user specified
        # backend is inappropriate for the given data)
        self._unpack_with_arguments()
        self._guess_backend()


def _build_kwargs_from_args(args=None, defaults=[], keys=[]):
    """ Build a standard dictionary from user-supplied *args or **kwargs
    function input """
    logger.debug("Input:%s",str(args))
    logger.debug("Type:%s",str(type(args)))
    if args is None : return None
    # standard interface is often a tuple that we need to unpack
    # as a list or dict
    if type(args) is tuple:
        args = args[0] if type(args[0]) is dict else list(args)
    kwargs = { }
    if type(args) is list:
        for i in range(len(keys)):
            try:
                if type(args[i]) is dict:
                    kwargs[keys[i]] = list(args[i].values())[0]
                else:
                    kwargs[keys[i]] = args[i]
            except IndexError:
                kwargs[keys[i]] = defaults[i]
    else:
        for i in range(len(keys)) : kwargs[keys[i]] = defaults[i]
        for key in kwargs.keys():
            if key in args.keys():
                kwargs[key] = args[key]
    logger.debug("Result:%s",str(kwargs))
    return kwargs