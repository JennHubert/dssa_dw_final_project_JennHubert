import pandas as pd
from JennSRC.clients.postgres import PostgresClient
from tasks import TaskContainer


DATABASECONFIG='.config/database.ini'


def create_cursor(path, section):
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor



task1=TaskContainer(create_cursor)
task1.run(path=DATABASECONFIG, section='postgresql') 




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