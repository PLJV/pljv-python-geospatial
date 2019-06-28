import json
from .do import _build_kwargs_from_args

class PostGis(object):
    def __init__(self, *args, **kwargs):
        # argument handlers
        KNOWN_ARGS = ['json_conf','table_name','host','port','username','password']
        DEFAULTS = [None,None]
        if len(args) > 0:
            kwargs = _build_kwargs_from_args(args, defaults=DEFAULTS, keys=KNOWN_ARGS)
        else:
            kwargs = _build_kwargs_from_args(kwargs, defaults=DEFAULTS, keys=KNOWN_ARGS)
        # default configuration options
        self.host = kwargs.get('host', None)
        self.port = kwargs.get('port', None)
        self.username = kwargs.get('username', None)
        self.password = kwargs.get('password', None)
        # if we were passed a json file input and 
        # the settings weren't specified by user 
        # explicitly, fill-them in here
        if kwargs['json_conf'] is not None:
            with open(kwargs['json_conf']) as file:
                json_conf = json.loads(file)
                self.host = kwargs.get('host', json_conf.get('host', None))
                self.port = kwargs.get('port', json_conf.get('port', None))
                self.username = kwargs.get('username', json_conf.get('username', None)) 
                self.password = kwargs.get('password', json_conf.get('password', None))
    def to_wkt(self):
        raise NotImplementedError
        