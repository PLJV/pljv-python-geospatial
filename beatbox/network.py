import json
import psycopg2

import geopandas as gp

class PostGis(object):
    def __init__(self, json_conf=None, **kwargs):
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.database_name = None
        self.table_name = None
        self.crs = '+init=epsg:4326'
        self._sql_query_string = None

        # if we were passed a json file input and 
        # the settings weren't specified by user 
        # explicitly, fill-them in here
        if json_conf is not None:
            with open(json_conf) as file:
                json_conf = json.loads(file.read())
                self.host = kwargs.get('host', 
                    json_conf.get('host', None))
                self.port = kwargs.get('port', 
                    json_conf.get('port', None))
                self.username = kwargs.get('username', 
                    json_conf.get('user', None)) 
                self.password = kwargs.get('password', 
                    json_conf.get('password', None))
                self.database_name = kwargs.get('database', 
                    json_conf.get('database', None)) 
                self.table_name = kwargs.get('table_name', 
                    json_conf.get('table', None))
                self.crs = kwargs.get('crs', 
                    json_conf.get('crs', self.crs))
        else:
            self.host = kwargs.get('host', None)
            self.port = kwargs.get('port', None)
            self.username = kwargs.get('username', None)
            self.password = kwargs.get('password', None)
            self.database_name = kwargs.get('database', None)
            self.table_name = kwargs.get('table_name', None)
       
        if kwargs.get('sql') is not None:
            self._sql_query_string = kwargs.get('sql')
        elif self.table_name is not None:
            self._sql_query_string = "SELECT * FROM " + self.table_name + ";"
        
        self.connect = psycopg2.\
            connect(database=self.database_name,host=self.host, 
        		user=self.username, password=self.password)
        		
        self.cursor = self.connect.cursor()

    @property
    def sql_query(self):
        return self._sql_query_string
    
    @sql_query.setter
    def sql_query(self, *args):
        self._sql_query_string = args[0]
    
    def to_wkt(self):
        raise NotImplementedError

    def read_table(self):
        if self.sql_query is None:
            raise AttributeError('SQL query is undefined. Cannot read table')
        try:
            self.cursor.execute(self.sql_query)
            df = self.cursor.fetchall()
        finally:
            self.cursor.close()
        
        return df

    def write_table(self):
        raise NotImplementedError

class QsCredentials(object):
    """
    QuickStats Credentials
    """
    def __init__(self, json_conf, api_key, *args):
        if not args:
            args = {}
        else:
          args = args[0]
        json_conf = args.get('json_conf', None)
        if json_conf is not None:
            with open(json_conf) as file:
                json_conf = json.loads(file)
                self.api_key = args.get('api_key', 
                    json_conf.get('api_key', None))
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
