import sqlglot
from BTrees.OOBTree import OOBTree

from dbms import DBMS, DB, Table

'''
sql = 'SELECT * FROM trips LIMIT 10;'

#res = Token(sql)
#tkns = res.tokenize()
#print(tkns)

print(sqlglot.parse(sql))

#index
#when user creates a select,
#select columns should be indexed for faster retrieval
#use that column to get rest of information
#join table first?? idk
#b tree structure

#t = OOBTree()


dbms = DBMS(); 
#CREATE DATABASE covid_app;
covid_app = dbms.create_db; 
#USE covid_app;
current_db = covid_app
#CREATE table -
current_table = current_db.create_table(table_name, columns, col_types)
#INSERT INTO
current_table.insert()
'''

dbms = DBMS(); 
#CREATE DATABASE covid_app;
covid_app = dbms.create_db('school_app') 

#USE covid_app;
current_db = covid_app

#CREATE table -
table_name = 'school_directory'
columns = ['name','age','grade']
col_types = ['int','int','int']
current_table = current_db.create_table(table_name, columns, col_types)

#INSERT INTO
current_table.insert(['jack','8','2'], ['name','age','grade'])
current_table.insert(['jill','10','3'], ['name','age','grade'])
current_table.insert(['jack','11','4'], ['name','age','grade'])


#WHERE
#print(self.col_btrees)
#current_table.where('eq','age','10')