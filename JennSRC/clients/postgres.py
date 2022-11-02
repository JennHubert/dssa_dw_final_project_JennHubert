import configparser
from psycopg import connect, Connection
from psycopg.conninfo import make_conninfo
from configparser import ConfigParser


class PostgresClient:
    '''
    Postgres client for working with postgres databses in python
    '''
    def __init__(self, host:str=None, port:int=None, user:str=None, password:str=None, dbname:str=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = dbname
        
    def connect_from_config(self, path:str, section:str, **kwargs) -> Connection:
        '''
        This makes a psycopg3 connection object from a config file
        
        The args: path(str) - path to the config file
        section(str)- name of section in the confid file
        
        It return a new connection instance
        
        '''
        
        conn_dict = {}
        config_parser = configparser()
        
        #Below reads the config file
        config_parser.read(path)
        if config_parser.has_section(section):
            config_params = config_parser.items(section)
            for k,v in config_params:
                conn_dict[k]=v
                
        conn = connect(
            conninfo=make_conninfo(**conn_dict),
            **kwargs
        )
        
        conn._check_connection_ok()

        return conn
    
    def connect(self, **kwargs) -> Connection:
        '''
        
        This creates a psycopg3 connection object from connection parameters
        passed as **kwargs. Alias for psycopg.connect()
        
        This Returns:
            Connection: a new connection instance
        
        '''
        
        conn = connect(   
            connfinfo=make_conninfo(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.database,
                **kwargs)
        )
        #This will check if the connection is okay - this will throw an error if not
        conn._check_connection_ok()
        
        return conn