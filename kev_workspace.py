# import sqlglot
# from BTrees.OOBTree import OOBTree
# import sqlglot
# from sqlglot.expressions import ColumnDef, Identifier, DataType, From, Star, Table, Values, Literal, Tuple, Where, EQ, Column
# from BTrees.OOBTree import OOBTree
# import pickle
# from dbms import DBMS
# from database import DB
# from table import Table
import sqlglot
from sqlglot.expressions import ColumnDef, Identifier, DataType, From, Star, Table, Values, Literal, Tuple, Where, EQ, Column
from BTrees.OOBTree import OOBTree
import pickle
from dbms import DBMS
from database import DB

from table import Table as CustomTable

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


# dbms = DBMS();
# #CREATE DATABASE covid_app;
# demo_app = dbms.create_db('demo_app')
#
# #USE covid_app;
# current_db = demo_app
#
# #CREATE table -
# table_name = 'rel_i_i_10000'
# columns = ['col1','col2']
# col_types = ['int','int']
# rel_i_i_10000 = current_db.create_table(table_name, columns, col_types)
# ## bulk insert
# print('INSERT')
# rel_i_i_10000_rows = rel_3()
# rel_i_i_10000.insert_bulk(rel_i_i_10000_rows,columns)
#
# table_name = 'rel_i_i_1000'
# columns = ['col1','col2']
# col_types = ['int','int']
# rel_i_i_1000 = current_db.create_table(table_name, columns, col_types)
# rel_i_i_1000_rows = rel_1()
# rel_i_i_1000.insert_bulk(rel_i_i_1000_rows,columns)
#
#
# table_name = 'rel_i_1_1000'
# rel_i_1_1000 = current_db.create_table(table_name, columns, col_types)
# rel_i_1_1000_rows = rel_2()
# rel_i_1_1000.insert_bulk(rel_i_1_1000_rows,columns)
#
# ## join
#
# inner_1000 = rel_i_i_1000.join(rel_i_1_1000, 'eq', 'col2','col2', 'inner')
# # inner_1000.print_table()
# left_1000 = rel_i_i_1000.join(rel_i_1_1000,'eq','col2','col2','left')
# right_1000 = rel_i_i_1000.join(rel_i_1_1000,'eq','col2','col2','right')
# # left_1000.print_table()
# # right_1000.print_table()
#
# rel_i_i_1000.update(['col1'],[200],['eq','col2',10])
# rel_i_i_1000.update(['col1'],[1111],['eq','col2',1])
# rel_i_i_1000.update(['col1'],[2222],['eq','col2',2])
# # rel_i_i_1000.print_table()
#
# rel_i_i_1000.insert([300,111],['col1','col2'])
# for i in range(10):
# 	rel_i_i_1000.insert([1000+i,700+i],['col1','col2'])
#
# # rel_i_i_1000.print_table()
# ### the rel_i_i_1000 has (200,10)(1111,1)(2222,2) but are not showing after the left join
# ### the inserted rows show correctly after the join
# ## will take a look to see what the problem is caused by
#
# print('-------')
# inner1000_10000 = rel_i_i_1000.join(rel_i_i_10000,'eq','col1','col1','left')
# inner1000_10000.print_table()


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
col_types = ['str','int']
height_tbl = current_db.create_table(table_name, columns, col_types)

height_tbl.insert(['jack','80'], ['name','height'])
height_tbl.insert(['jill','100'], ['name','height'])
height_tbl.insert(['max','95'], ['name','height'])
height_tbl.insert(['kim','95'], ['name','height'])

# height_tbl.print_table()

# height_tbl.delete(['eq','height','95'])
height_tbl.print_table()
print('00000')
gr_tbl = height_tbl.groupby('height')
gr_tbl.print_table()
min_row = gr_tbl.min('height')
print(min_row)

def where(res):
	where_dict = {}
	res = res.args['this']
	where_dict['operation'] = res.key

	where_dict['operand_l'] = res.args['this'].args['this'].args['this']
	if res.args['expression'].find(Column) == None:
		where_dict['operand_r'] = res.args['expression'].args['this']
	else:
		where_dict['operand_r'] = res.args['expression'].args['this'].args['this']
	return where_dict

def update(res):
	table_name = res.find(Table).args['this'].args['this']
	expr = res.args['expressions']
	cols = []
	wheres = res.args['where']
	col_vals = []
	where_val = None
	if wheres != None:
		where_val = where(wheres)
	if len(expr) == 1:
		cols = [expr[0].find(Identifier).args['this']]
		col_vals = [expr[0].find(Literal).args['this']]
	else:
		for col in expr:
			cols.append(col.find(Identifier).args['this'])
			col_vals.append(col.find(Literal).args['this'])
	print(cols)
	print(col_vals)
	return table_name, cols, col_vals, where_val

def test_update(sql_str):
	res = sqlglot.parse_one(sql_str)
	table_name, cols, col_vals, where_val = update(res)
	sel_tbl = current_db.tables.get(table_name)  # from
	where_rows = sel_tbl.where(where_val['operation'], where_val['operand_l'], where_val['operand_r'])
	sel_tbl.update(cols, col_vals, where_rows)


def join(res):
	join_dict = {}

	res = res[0]
	join_dict['type'] = res.args['kind']
	join_dict['Table'] = res.find(Table).args['this'].args['this']

	res = res.args['on']
	join_dict['operation'] = res.key

	join_dict['operand_l'] = res.args['this'].args['this'].args['this']
	if res.args['expression'].find(Column) == None:
		join_dict['operand_r'] = res.args['expression'].args['this']
	else:
		join_dict['operand_r'] = res.args['expression'].args['this'].args['this']

	#print(join_dict)
	return join_dict

def groupby(res):
	col_name = res.find(Identifier).args['this']
	return col_name

def select(res):
	res = res.args
	print(res.keys())

	join_val = None
	print(res['joins'])
	if len(res['joins']) > 0:
		join_val = join(res['joins'])

	where_val = None
	if res['where'] != None:
		where_val = where(res['where'])
	if res['group'] != None:
		group_col = groupby(res['group'])
	table_name = res['from'].args['expressions'][0].args['this'].args['this']

	expr = res['expressions']
	cols = []
	if len(expr) == 1:
		expr = expr[0]
		is_star = expr.find(Star)
		if is_star is not None:
			cols = ['star']
		else:
			cols = [expr.find(Identifier).args['this']]
	else:

		for col in expr:
			cols.append(col.find(Identifier).args['this'])

	print('Table name:',table_name)
	print('Columns:',cols)
	print('Where:',where_val)
	print('Join:',join_val)
	print('Groupby:',group_col)

	return table_name, cols, where_val, join_val, group_col

def test_select(sql_str):
	res = sqlglot.parse_one(sql_str)
	table_name, cols, where_val, join_val, group_col = select(res)
	sel_tbl = current_db.tables.get(table_name)  # from
	if where_val != None:  # where
		where_rows = sel_tbl.where(where_val['operation'], where_val['operand_l'], where_val['operand_r'])

		new_name = sel_tbl.table_name + '_temp'
		new_tbl = Table(new_name, sel_tbl.columns, sel_tbl.col_types)
		new_tbl.insert_bulk(where_rows, sel_tbl.columns)
		sel_tbl = new_tbl

	# select - cols
	new_name = sel_tbl.name + '_temp'
	new_col_types = []
	for c in cols:
		ind = sel_tbl.columns.index(c)
		new_col_types.append(sel_tbl.col_types[ind])

	new_tbl = CustomTable(new_name, cols, new_col_types)

	col_inds = []
	for c in cols:
		col_inds.append(sel_tbl.columns.index(c))

	first_col = cols[0]
	first_col_keys = list(sel_tbl.col_btrees[first_col].keys())
	for k in first_col_keys:
		res_rows = sel_tbl.col_btrees[first_col].get(k)
		print(res_rows)
		for i in range(len(res_rows)):
			new_row = []
			for j in col_inds:
				new_row.append(res_rows[i].values[j])

			res_rows[i] = new_row

		# print(new_tbl.columns)
		new_tbl.insert_bulk(res_rows, new_tbl.columns)
	new_tbl.print_table()
	if group_col != None:
		new_tbl = new_tbl.groupby(group_col)
	print('hahahah')
	new_tbl.print_table()
	return 'Select done'


# sql_str = "UPDATE height_tbl SET name = 'john' WHERE height = '95';"
# sql_str_1 = "UPDATE height_tbl SET height = '120' WHERE name = 'jack';"
# print('original table')
# height_tbl.print_table()
# # print(sqlglot.parse_one(sql_str))
# print('')
# test_update(sql_str)
# print('updated table')
# height_tbl.print_table()
#
# print('original table')
# height_tbl.print_table()
# # print(sqlglot.parse_one(sql_str))
# print('')
# test_update(sql_str_1)
# print('updated table')
# height_tbl.print_table()
print('1111111')
school_tbl.print_table()
print('old table')
sql_str = "SELECT name, grade, age FROM school_directory GROUP BY grade;"
print(test_select(sql_str))

# print('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')
# print(height_tbl.name)
# res = sqlglot.parse_one(sql_str)
# print('original table')
# height_tbl.print_table()
# table_name, cols, col_vals, where_val = update(res)
# where_rows = height_tbl.where(where_val['operation'], where_val['operand_l'], where_val['operand_r'])
# print('')
# height_tbl.update(cols, col_vals, where_rows)
# print('updated table')
# height_tbl.print_table()
# test_update(sql_str)