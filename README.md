# Data Gathering & Warehousing Final Project
---

This repository contains code that functions as an ***ETL process***

It takes data from a highly normalized transactional database and transforms it into a data warehouse in a star schema for optimizied queries.

This code can be used as a guide for creating your own data warehouse.


---
The following python libraries were crucial in completing this assignement

1. ***Pandas***
2. ***SQLAlchemy***
3. ***NetworkX***
4. ***psycopg***

---
## Project Structure

### Below I decsribe where and why things are located there they are.
*   `.config` - This folder for configuration files (Example: `.json`, `.ini`)
*   `images` - This folder hold PNG images used for this readme page.
*   `JennSRC` - This is the source code folder containing all application code and modules.

*   `main.py` - This script is the entry point to the ETL process
*   `README` - Markdown file describing the project
*   `requirements.txt` - list of python libraries to install with `pip`
*   `Table Definitions` - Descriptions of the tables in the newly created data warehouse


---
### Below is an image of the star schema


![img](images/StarSchema.png)

---
### Below is the code for the functions I used to complete the ***Extract*** and ***Load*** portions of this project
``` python 
# Takes care of the "Extract" part  of ETL
def read_table(sql, con) -> pd.DataFrame:
    """Executes a query and retrieves data from the database

    Args:
        sql (string): SQL query
        con (Connection): a connection instance

    Returns:
        df (dataframe): a pandas dataframe
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
        None: a pandas dataframe to be loaded into data warehouse
    """
    df.to_sql(name=name, con=con, if_exists=if_exist, schema=schema, index=False, method='multi')
    return None
```
---

### Below is an image of my DAG

![img](images/DAG.png)

---
### Below is the code I utilized to create the above image

``` python 
nodes = [(task1, task2), (task2, task3), (task3, task4), (task4, task5), (task5, task6), (task6, task7), (task7, task8), (task8, task9), (task9, task10), (task10, task11), (task11, task12), (task12, task13),(task13, task14), (task14, task15), (task15, task16), (task16, task17), (task17, task18), (task18, task19), (task19, task20), (task20, task21), (task21, task22), (task22, task23), (task23, task24),(task24, task25)             
    ]
    
    DAG = nx.DiGraph(nodes)
   
    nx.draw(DAG)