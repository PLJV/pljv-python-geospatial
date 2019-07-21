import json

class PostGis(object):
    def __init__(self, json_conf, table_name, host, port, username, password, *args):
        if not args:
            args = {}
        json_conf = args.get('json_conf', None)
        table_name = args.get('table_name', None)
        # if we were passed a json file input and 
        # the settings weren't specified by user 
        # explicitly, fill-them in here
        if json_conf is not None:
            with open(json_conf) as file:
                json_conf = json.loads(file)
                self.host = args.get('host', json_conf.get('host', None))
                self.port = args.get('port', json_conf.get('port', None))
                self.username =args.get('username', json_conf.get('username', None)) 
                self.password = args.get('password', json_conf.get('password', None))
        else:
            host = args.get('host', None)
            port = args.get('port', None)
            username = args.get('username', None)
            password = args.get('password', None)
    def to_wkt(self):
        raise NotImplementedError

class QsCredentials(object):
    """
    QuickStats Credentials
    """
    def __init__(self, json_conf, api_key, *args):
        if not args:
            args = {}
        json_conf = args.get('json_conf', None)
        if json_conf is not None:
            with open(json_conf) as file:
                json_conf = json.loads(file)
                self.api_key = args.get('api_key', json_conf.get('api_key', None))
        else:
            api_key = args.get('api_key', None)


class QuickStats(object):
    """
    Interface for the USDA-NASS-QuickStats Service
    """
    def __init__(self, params, api_key, *args):
        raise NotImplementedError
    def get(self):
        """
        Issues a QuickStats-comprehensible HTTP(S) GET request 
        e.g.,  https://quickstats.nass.usda.gov/api/get_counts/?key=api key&commodity_desc=CORN&year__GE=2012&state_alpha=VA
        """