import os
import pandas as pd
from tasks import TaskContainer
from pypika import Schema
from JennSRC.clients.postgres import PostgresClient
from JennSRC.common.queue import QueueFactory
from pypika import PostgreSQLQuery as Q
from sqlalchemy import create_engine  
import networkx as nx
from networkx import (
    is_directed_acyclic_graph,
    is_weakly_connected,
    number_of_nodes,
    is_empty,
    topological_sort
)

host = os.environ["host"]
port = os.environ["port"]
user = os.environ["user"]
password = os.environ["password"]
db = os.environ["db"]
dbtype = os.environ["dbtype"]

SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"

conn = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)







#import pandas as pd
#import psycopg2
#from psycopg import Cursor
#from pypika import Schema, Column
#from pypika import PostgreSQLQuery as Q
#from JennSRC.clients.postgres import PostgresClient
#from tasks import TaskContainer
#from JennSRC.common.queue import QueueFactory


#Parameters

DATABASECONFIG='.config/database.ini'
SECTION='postgresql'
DW = Schema('dssa')
TASKQUEUE=QueueFactory.factory()

#~~~~~~~~~~~~~ FUNTCTIONS ~~~~~~~~~~~~~~~#  

#   The code below are the python functions that is the code
#   that conducts the ETL Process using SQLAlchemy and pandas.
#   This code moves the data from the database to the data warehouse
#   and builds the star schema!
 


def create_conn(path, section):
    """Creates a Connection to the Database 
       for sending commands and queries.

    Args:
        path (str): path to an ini file w/ the database paramaters
        section (str): section name in the ini file w/ parameters

    Returns:
        Connection: A Connection instance
    """
    client = PostgresClient()
    con = client.connect_from_config(path, section, autocommit=True)
    #cursor = conn.cursor()
    return con

# Takes care of the "Extract" part  of ETL
def read_table(sql, con) -> pd.DataFrame:
    """Executes a query and retrieves data from the database

    Args:
        sql (_type_): _description_
        con (Connection): a connection instance

    Returns:
        pd.DataFrame: _description_
    """
    df = pd.read_sql(sql=sql, con=con)
    return df

# Takes care of the "Load" part  of ETL
def write_table(df:pd.DataFrame, name, con, schema, if_exist='replace') -> pd.DataFrame:
    """Loads the data into the data warehouse.

    Args:
        df (pd.DataFrame): df the data is stored in
        name (str): name of table the data will be loaded into
        con (Connection): a connection instance
        schema (str): name of schema that table will be located
        if_exist (str, optional): Defaults to 'replace'.

    Returns:
        pd.DataFrame: dataframe to be loaded into data warehouse
    """
    df.to_sql(name=name, con=con, if_exists=if_exist, schema=schema, index=False, method='multi')
    return None


# ~~~ Below are the Transformation functions ~~~ #


def transform_customer(df):
    
    df.rename(columns={'customer_id': 'sk_customer'}, inplace=True)
    df['name'] = df.first_name + " " + df.last_name
    dim_customer = df[['sk_customer', 'name', 'email']].copy()
    dim_customer.drop_duplicates(inplace=True)
    return dim_customer


def transform_staff(df):
   
    df.rename(columns={'staff_id': 'sk_staff'}, inplace=True)
    df['name'] = df.first_name + " " + df.last_name
    dim_staff = df[['sk_staff','name', 'email']].copy()
    dim_staff.drop_duplicates(inplace=True)
    return dim_staff


def transform_film(df,df2):
   
    df.rename(columns={'film_id': 'sk_film','rating': 'rating_code','length': 'film_duration'}, inplace=True)
    df2.rename(columns={'language': 'name'}, inplace=True)
    dim_film=df.merge(right=df2, how='inner', on='language_id')
    dim_film=dim_film[['sk_film', 'rating_code', 'film_duration','rental_duration', 'name', 'release_year','title']]
    dim_film.copy()
    dim_film.drop_duplicates(inplace=True)
    return dim_film


def transform_store(store_df,staff_df,address_df,city_df,country_df):
    
    store_df.rename(columns={'store_id':'sk_store', 'manager_staff_id': 'staff_id'}, inplace=True)
    staff_df['name']= staff_df.first_name + " " + staff_df.last_name
    staff_df = staff_df[['staff_id', 'name']].copy()
    country_df= country_df[['country_id', 'country'].copy()]
    city_df = city_df[['city_id', 'city', 'country_id']].copy()
    city_df = city_df.merge(country_df, how='inner', on='country_id')
    address_df = address_df[['address_id', 'address', 'district', 'city_id']].copy()
    address_df = address_df.merge(city_df, how='inner', on='city_id')
    address_df.rename(columns={'district':'state'}, inplace=True)
    store_df= store_df.merge(staff_df,how='inner',on='staff_id')
    store_df= store_df.merge(address_df, how='inner', on='address_id')
    dim_store=store_df[['sk_store', 'name', 'address', 'city', 'state','country']].copy()
    return dim_store


def transform_factrental(rental_df,inventory_df,dim_date,dim_film,dim_staff,dim_store):
    
    rental_df.rename(columns={'customer_id':'sk_customer', 'rental_date':'date'}, inplace=True)
    rental_df['date'] = rental_df.date.dt.date
    rental_df = rental_df.merge(dim_date, how='inner', on='date')
    rental_df = rental_df.merge(inventory_df, how='inner', on='inventory_id')
    rental_df = rental_df.merge(dim_film, how='inner', left_on='film_id', right_on='sk_film')
    rental_df = rental_df.merge(dim_staff, how='inner', left_on='staff_id', right_on='sk_staff')
    rental_df.rename(columns={'name_y':'name'}, inplace=True)
    rental_df = rental_df.merge(dim_store, how='inner', on='name')
    rental_df = rental_df.groupby(['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff']).agg(count_rentals=('rental_id','count')).reset_index()
    dim_factrental = rental_df[['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff', 'count_rentals']].copy()
    return dim_factrental

def transform_date(date_df):

    date_df['sk_date'] = date_df.rental_date.dt.strftime("%Y%m%d").astype('int')
    date_df['date'] = date_df.rental_date.dt.date
    date_df['quarter'] = date_df.rental_date.dt.quarter
    date_df['year'] = date_df.rental_date.dt.year
    date_df['month'] = date_df.rental_date.dt.month
    date_df['day'] = date_df.rental_date.dt.day
    dim_date = date_df[['sk_date', 'date', 'quarter', 'year', 'month', 'day']].copy()
    dim_date.drop_duplicates(inplace=True)
    return dim_date



def main():
    conn = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
    
    
    #Customer Table Creation
    customer = read_table(sql='SELECT * FROM public.customer', con=conn) 
    dim_customer = transform_customer(df=customer)
    load_dim_customer = write_table(df=dim_customer, con=conn, name='dim_customer', schema='dssa', if_exist='replace')
    
    
    #Staff Table Creation
    staff=read_table(sql='SELECT * from public.staff', con=conn)
    dim_staff=transform_staff(df=staff)
    load_dim_staff = write_table(df=dim_staff, con=conn, name='dim_staff', schema='dssa', if_exist='replace')
    
    
    #Film Table Creation
    film=read_table(sql='SELECT * from public.film', con=conn)
    language=read_table(sql='SELECT * from public.language', con=conn)
    dim_film=transform_film(df=film,df2=language)
    load_dim_film=write_table(df=dim_film, con=conn, name='dim_film', schema='dssa', if_exist='replace')
    
    
    #Date Table Creation
    date= read_table(sql='SELECT *  FROM public.rental',con=conn)
    dim_date=transform_date(date_df=date)
    load_dim_date=write_table(df=dim_date,con=conn,name='dim_date',schema='dssa',if_exist='replace')
    
    
    #Store Table Creations
    store = read_table(sql='SELECT * FROM public.store', con=conn)
    staff= read_table(sql='SELECT * from public.staff', con=conn)
    address = read_table(sql='SELECT * from public.address', con=conn)
    city = read_table(sql='SELECT * from public.city', con=conn)
    country= read_table(sql='SELECT * FROM public.country', con=conn)
    dim_store= transform_store(store_df=store,staff_df=staff,address_df=address,city_df=city,country_df=country)
    load_dim_store= write_table(df=dim_store,con=conn, name='dim_store', schema='dssa', if_exist='replace')
    
    
    #Fact Rental Table Creation
    rental = read_table(sql='SELECT * FROM public.rental', con=conn)
    inventory = read_table(sql='SELECT * FROM public.inventory',con=conn)
    dim_factrental = transform_factrental(rental_df=rental, inventory_df= inventory, dim_date=dim_date, dim_film=dim_film,dim_staff=dim_staff,dim_store=dim_store)
    load_dim_factrental=write_table(df=dim_factrental,con=conn,name='fact_rental', schema='dssa',if_exist='replace')
    

    #Create Connection
    task1=TaskContainer(create_conn)
    task1.run(path=DATABASECONFIG, section='postgresql')
    #Create Customer Table
    task2=TaskContainer(read_table)
    task2.run(sql='SELECT * FROM public.customer', con=conn)
    task3=TaskContainer(transform_customer)
    task3.run(df=customer)
    task4=TaskContainer(write_table)
    task4.run(df=dim_customer, con=conn, name='dim_customer', schema='dssa', if_exist='replace')
    #Create Staff Table
    task5=TaskContainer(read_table)
    task5.run(sql='SELECT * from public.staff', con=conn)
    task6=TaskContainer(transform_staff)
    task6.run(df=staff)
    task7=TaskContainer(write_table)
    task7.run(df=dim_staff, con=conn, name='dim_staff', schema='dssa', if_exist='replace')
    
    nodes = [(task1, task2), (task2, task3), (task3, task4), (task4, task5), (task5, task6), (task6, task7)    
    ]
    
    DAG = nx.DiGraph(nodes)
    print(DAG)
    
    
    assert is_directed_acyclic_graph(DAG) == True
    #is_weakly_connected(DAG)
    assert is_directed_acyclic_graph(DAG) == True
    assert is_empty(DAG) == False
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

#task1=TaskContainer(create_cursor)
#task1.run(path=DATABASECONFIG, section='postgresql')
'''
task1=TaskContainer(create_conn)
task1.run(path=DATABASECONFIG, section='postgresql')
task2=TaskContainer(read_table)
task2.run(sql='SELECT * FROM public.customer', con=conn)
task3=TaskContainer(transform_customer)
task3.run(df=customer)
'''










if __name__ == '__main__':
    main()