import pandas as pd
import psycopg2
from psycopg import Cursor
from pypika import Schema, Column
from pypika import PostgreSQLQuery as Q
from JennSRC.clients.postgres import PostgresClient
from tasks import TaskContainer

#Parameters

DATABASECONFIG='.config/database.ini'
SECTION='postgresql'
DW = Schema('dssa')


#Table Defs
FACT_RENTAL = (
    Column('sk_customer', 'INT', False),
    Column('sk_date', 'INT', False),
    Column('sk_store', 'INT', False),
    Column('sk_film', 'INT', False),
    Column('sk_staff', 'INT', False),
    Column('count_rentals', 'INT', False),  
)

DIM_CUSTOMER = (
    Column('sk_customer', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'INT', False),   
)

DIM_STAFF = (
    Column('sk_staff', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False),
)


DIM_FILM = (
    Column('sk_film', 'INT', False),
    Column('rating_code', 'VARCHAR(100)', False),
    Column('film_duration', 'INT', False),
    Column('rental_duration', 'INT', False),
    Column('language', 'CHAR(20)', False),
    Column('release_year', 'INT', False),
    Column('title', 'VARCHAR(225)', False),
)


DIM_DATE = (
    Column('sk_date', 'TIMESTAMP', False),
    Column('quarter', 'INT', False),
    Column('year', 'INT', False),
    Column('month', 'INT', False),
    Column('day', 'INT', False),
)

DIM_STORE = (
    Column('sk_store', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('address', 'VARCHAR(50)', False),
    Column('city', 'VARCHAR(50)', False),
    Column('state', 'VARCHAR(20)', False),
    Column('country', 'VARCHAR(50)', False),
)

#Functions

def create_cursor(path, section):
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor

def create_schema(cursor: Cursor, schema_name:str):
    q= f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    cursor.execute(q)
    return cursor

def create_table(
    cursor:Cursor,
    table_name:str,
    definition:tuple,
    primary_key:str=None,
    foreign_keys:list=None,
    reference_tables:list=None):
    
    dd1= Q \
        .create_table(table_name) \
        .if_not_exists() \
        .columns(*definition)
        
    if primary_key is not None:
        dd1 = dd1.primary_key(primary_key)
        
    if foreign_keys is not None:
        for idx, key in enumerate(foreign_keys):
            dd1.foreign_key(
                coulmns=key,
                reference_table=[idx],
                reference_columns=key
            )
    dd1=dd1.get_sql()

    cursor.execute(dd1)
    return cursor
    
def tear_down(cursor: Cursor):
    cursor.execute('Drop Schema DSSA Cascade;')
    cursor.close()
    return


#task1=TaskContainer(create_cursor)
#task1.run(path=DATABASECONFIG, section='postgresql')


def main()
'''
#Make Task Class - container for the functions that executes the functions
#^^just running the functions
from typing import List, Tuple

# I am packing this into a function that we can encapsulate within a set of task
def setup_client(config_file, section):
    client=PostgresClient
    cursor=client.connect_from_config(path=config_file, section=section)
    return cursor

'''
'''
#GARBAGE BELOW
def create_new_schema(cursor, schema)-> Connection:
    cursor.execute(f"SET search_path to {catalog}, {schema}:")
    return cursor
    #I don't think I need this because I created a schema already

'''

'''
def get_search_path(cursor, catalog, schema) -> Connection:
    cursor.execute('SET search_path TO {catalog}, {schema}:')
    return cursor

def read_table(cursor, table) -> List[Tuple]:
    tbl=cursor.execute(f"SELECT * FROM {table};").fetchall()
    return tbl

def to_pandas_df(data) -> pd.DataFrame:
    df=pd.DataFrame(data)
    return df

tsk_0=Task(setup_client)
tsk_1=Task(set_search_path)
#tsk_2=Task(create_new_schema)
tsk_3=Task(read_table)
tsk_4=Task(to_pandas_df)


cursor=tsk_0.run('../../JENNSRC\.config/postgres.py', 'postgresql')
#I think the '.postgres.py is referring to the file in the config folder right?
cursor=tsk_1.run(cursor=cursor, catalog='dvdrental', schema='public')
data=tsk_3.run(cursor=cursor, table='actor')
df=tsk_4.run(data)
df.head
'''
if __name__ == '__main__':
    main()