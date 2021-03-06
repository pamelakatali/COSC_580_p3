import sqlglot
from BTrees.OOBTree import OOBTree

from dbms import DBMS
from database import DB
from table import Table

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
school_tbl = current_db.create_table(table_name, columns, col_types)

#INSERT INTO
print('INSERT')
school_tbl.insert(['jack','8','2'], ['name','age','grade'])
school_tbl.insert(['jill','10','3'], ['name','age','grade'])
school_tbl.insert(['jack','11','4'], ['name','age','grade'])
school_tbl.print_table()

#WHERE
print('WHERE')
#print(self.col_btrees)
school_tbl.where('eq','name','jack')

#CREATE table -
table_name = 'height_tbl'
columns = ['name','height']
col_types = ['int','int']
height_tbl = current_db.create_table(table_name, columns, col_types)

height_tbl.insert(['jack','80'], ['name','height'])
height_tbl.insert(['jill','100'], ['name','height'])
height_tbl.insert(['jack','95'], ['name','height'])

#JOIN
print('LEFT JOIN')
join_tbl = school_tbl.join(height_tbl, 'eq', 'name','name', 'left')
join_tbl.print_table()
join_tbl.print_col_b_trees()

print('LEFT OUTER JOIN')
school_tbl.insert(['jim','10','3'], ['name','age','grade'])
join_tbl = school_tbl.join(height_tbl, 'eq', 'name','name')
join_tbl.print_table()
join_tbl.print_col_b_trees()


print('RIGHT OUTER JOIN')
height_tbl.insert(['jane','97'], ['name','height'])
join_tbl = school_tbl.join(height_tbl, 'eq', 'name','name','right')
join_tbl.print_table()


print('RIGHT OUTER')
outer_tbl, inner_tbl = school_tbl.right_outer_join(height_tbl, 'eq', 'name','name')
outer_tbl.print_table()


print('RIGHT INNER')
inner_tbl.print_table()

print('LEFT OUTER')
outer_tbl, inner_tbl = school_tbl.left_outer_join(height_tbl, 'eq', 'name','name')
outer_tbl.print_table()
print('LEFT INNER')
inner_tbl.print_table()

print('INNER JOIN')
join_tbl = school_tbl.join(height_tbl, 'eq', 'name','name', 'inner')
join_tbl.print_table()
#join_tbl.print_col_b_trees()

print('FULL JOIN')
join_tbl = school_tbl.join(height_tbl, 'eq', 'name','name', 'full')
join_tbl.print_table()
#join_tbl.print_col_b_trees()
'''
'''
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

def bulk_load():
	result = rel_3()
	print(result)
	print(len(result))


#bulk_load()

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

print('INSERT')
rel_i_i_10000_rows = rel_4()
rel_i_i_10000.insert_bulk(rel_i_i_10000_rows,columns)
rel_i_i_10000.print_table()
'''

[<Type.VARCHAR: 'VARCHAR'>, <Type.INT: 'INT'>, <Type.INT: 'INT'>]
##########
CREATE DATABASE school_app
USE school_app
CREATE TABLE school_directory (name VARCHAR, age INT, grade INT, gpa DOUBLE)
INSERT INTO school_directory (name, age, grade) VALUES ('jack', 8, 2, 3.3)
INSERT INTO school_directory (name, age, grade) VALUES ('jill', 10, 3, 3.5)
INSERT INTO school_directory (name, age, grade) VALUES ('jack', 11, 4, 3.7)
SELECT name, age FROM school_directory
SELECT name, grade FROM school_directory
SELECT name, age FROM school_directory WHERE age = 10
SELECT name, age FROM school_directory WHERE age >= 10
SELECT name, age FROM school_directory WHERE age <= 10
SELECT name, age FROM school_directory WHERE gpa >= 3.4
SELECT * FROM school_directory
SELECT * FROM school_directory LIMIT 10
SELECT * FROM school_directory LIMIT 1

INSERT INTO school_directory (name, age, grade) VALUES ('jim', 11, 4, 3.6)
SELECT * FROM school_directory


#joins
CREATE TABLE height_tbl (name VARCHAR, height INT)
INSERT INTO height_tbl (name, height) VALUES ('jack', 80)
INSERT INTO height_tbl (name, height) VALUES ('jill', 100)
INSERT INTO height_tbl (name, height) VALUES ('jack', 95)
INSERT INTO height_tbl (name, height) VALUES ('jane', 95)

SELECT name, age, grade, height FROM school_directory LEFT JOIN height_tbl ON name = name
SELECT name, age, grade, height FROM school_directory RIGHT JOIN height_tbl ON name = name
SELECT name, age, grade, height FROM school_directory INNER JOIN height_tbl ON name = name
SELECT name, age, grade, height FROM school_directory CROSS JOIN height_tbl ON name = name



SELECT name, age, grade FROM school_directory LEFT JOIN height_tbl ON name = name
SELECT name, age, grade, height FROM school_directory LEFT JOIN height_tbl ON name = name



UPDATE school_directory SET age = 12 WHERE name = 'jane'
UPDATE school_directory SET age = 12 WHERE gpa = 3.6



SELECT MIN(age), name FROM school_directory
SELECT MAX(age), name FROM school_directory
SELECT * FROM school_directory ORDER BY age
SELECT * FROM school_directory ORDER BY gpa ASC
SELECT age, name, grade FROM school_directory ORDER BY age DESC
SELECT MIN(age), name FROM school_directory ORDER BY age
SELECT MAX(age), name, grade FROM school_directory ORDER BY grade
SELECT age, gpa, grade FROM school_directory ORDER BY gpa DESC LIMIT 3
SELECT SUM(gpa) FROM school_directory
SELECT count(name) FROM school_directory
SELECT AVG(grade) FROM school_directory 



DELETE FROM school_directory WHERE age = 8
DELETE FROM school_directory WHERE gpa < 3.7

SELECT * FROM school_directory WHERE age=8 and gpa<3.4
SELECT * FROM school_directory Where age=8 or gpa<=3.5