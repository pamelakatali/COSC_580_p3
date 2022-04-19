import sqlglot
from BTrees.OOBTree import OOBTree

from dbms import DBMS
from database import DB
from table import Table

def rel_1():
	rel_i_i_1000 = []
	for i in range(1,1001):
		tpl = [i,i]
		rel_i_i_1000.append(tpl)
	return rel_i_i_1000

def rel_2():
	rel_i_1_1000 = []
	for i in range(1,1001):
		tpl = [i,1]
		rel_i_1_1000.append(tpl)
	return rel_i_1_1000

def rel_3():
	rel_i_i_10000 = []
	for i in range(1,10001):
		tpl = [i,i]
		rel_i_i_10000.append(tpl)
	return rel_i_i_10000

def rel_4():
	rel_i_1_10000 = []
	for i in range(1,10001):
		tpl = [i,1]
		rel_i_1_10000.append(tpl)
	return rel_i_1_10000


dbms = DBMS();
#CREATE DATABASE covid_app;
demo_app = dbms.create_db('demo_app')

#USE covid_app;
current_db = demo_app

#CREATE table -
table_name = 'rel_i_i_10000'
columns = ['col1','col2']
col_types = ['int','int']
rel_i_i_10000 = current_db.create_table(table_name, columns, col_types)
## bulk insert
print('INSERT')
rel_i_i_10000_rows = rel_3()
rel_i_i_10000.insert_bulk(rel_i_i_10000_rows,columns)

table_name = 'rel_i_i_1000'
columns = ['col1','col2']
col_types = ['int','int']
rel_i_i_1000 = current_db.create_table(table_name, columns, col_types)
rel_i_i_1000_rows = rel_1()
rel_i_i_1000.insert_bulk(rel_i_i_1000_rows,columns)


table_name = 'rel_i_1_1000'
rel_i_1_1000 = current_db.create_table(table_name, columns, col_types)
rel_i_1_1000_rows = rel_2()
rel_i_1_1000.insert_bulk(rel_i_1_1000_rows,columns)

## join

inner_1000 = rel_i_i_1000.join(rel_i_1_1000, 'eq', 'col2','col2', 'inner')
# inner_1000.print_table()
left_1000 = rel_i_i_1000.join(rel_i_1_1000,'eq','col2','col2','left')
right_1000 = rel_i_i_1000.join(rel_i_1_1000,'eq','col2','col2','right')
# left_1000.print_table()
# right_1000.print_table()

rel_i_i_1000.update(['col1'],[200],['eq','col2',10])
rel_i_i_1000.update(['col1'],[1111],['eq','col2',1])
rel_i_i_1000.update(['col1'],[2222],['eq','col2',2])
# rel_i_i_1000.print_table()

rel_i_i_1000.insert([300,111],['col1','col2'])
for i in range(10):
	rel_i_i_1000.insert([1000+i,700+i],['col1','col2'])

# rel_i_i_1000.print_table()
### the rel_i_i_1000 has (200,10)(1111,1)(2222,2) but are not showing after the left join
### the inserted rows show correctly after the join
## will take a look to see what the problem is caused by

print('-------')
inner1000_10000 = rel_i_i_1000.join(rel_i_i_10000,'eq','col1','col1','left')
inner1000_10000.print_table()
