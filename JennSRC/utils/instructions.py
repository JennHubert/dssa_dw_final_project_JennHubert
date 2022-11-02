'''

3. Create an ETL Pipeline Script called main.py in the app directory that has the following functions:

setup(): should initializes database sessions, and create all the schema, tables, fields, and relationships defined in your bases models from the previous step.
    1: Create connection to dvdrental using postgres client > should return cursor object
    2: create a connection to DSSA (data warehouse) > return cursor object to use in data pipeline
    3: Create all the tables I'll need in the star schema and the definitions in PGADMIN
extract(): should read from the desired database. You are responsible for setting the arguments of the function and determining what type of object should be returned.
    A set of extractions -read from table, should
    - select columns from table
    -use sql statement to get data and put that into a pandas dataframe
transform(): You can define any number of transformation related functions to accomplish the task using any arguments you come up with. Remember it is often better to debug if we isolate certain aspects of our data transformations.
    - multiple transformations (but I'll use a function multiple times)
load(): should insert data to the target database (the data warehouse). Again, you have the freedom to determine your own arguments to accomplish this task

teardown(): Should close any active sessions and open connections to the database.

'''