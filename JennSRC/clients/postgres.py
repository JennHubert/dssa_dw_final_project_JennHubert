import configparser
from psycopg import connect, Connection
from psycopg.connifo import make_conninfo
from configparser import ConfigParser


class PostgresClient:
    
    def __init__(self, host:str=None, port:int=None, user:str=None, password:str=None, dbname:str=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        
    def connect_from_config(self, path:str, section:str, **kwargs) -> Connection:
        
        
        conn_dict = {}
        config_parser = configparser()
        
        
        config_parser.read(path)
        if config_parser.has_section(section):
            config_params = config_parser.items(section)
            for k,v in confirg_params:
                conn_dict[k]=v
                
        conn = connect(
            conninfo=make_conninfo(**conn_dict),
            **kwargs
        )
        
        conn._check_connection_ok()

        return conn