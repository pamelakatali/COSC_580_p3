import sqlglot
from sqlglot.expressions import ColumnDef, Identifier, DataType, From, Star, Table, Values, Literal, Tuple, Where, EQ, Column
from BTrees.OOBTree import OOBTree
import pickle

from table import Table as CustomTable
import copy


#CREATE TABLE - if res.key == 'create'
def create(res):
	res = res
	table_name = res.find(Table).args['this'].args['this']
	cols = []
	col_types = []

	res_gen = res.args['this'].args['expressions']
	for comm in res_gen:
		idt = comm.find(Identifier)
		col_name = idt.args['this']
		dtyp = comm.find(DataType)
		col_type = dtyp.args['this']

		cols.append(col_name)
		col_types.append(col_type)
	print('Table name:', table_name)
	print('Columns:',cols)
	print('Column types:',col_types)

	return table_name, cols, col_types


## UPDATE this if res.key == 'update'
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

#DROP Table
def drop_table(res):
	table_name = res.find(Table).args['this'].args['this']
	print(table_name)


#INSERT INTO - add row to table
def insert(res):
	table_name = res.find(Table).args['this'].args['this']
	#print(res.args.keys())
	print(res.args['expression'].find(Tuple).args['expressions'])#.args['expressions'])

	cols_gen = res.args['this'].args['expressions']
	vals_gen = res.args['expression'].find(Tuple).args['expressions']
	cols = []
	vals = []

	for col in cols_gen:
		cols.append(col.args['this'])

	for val in vals_gen:
		vals.append(val.args['this'])

	print('Table name:', table_name)
	print('Columns:',cols)
	print('Column values:',vals)

	return table_name, cols, vals


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

	return table_name, cols, where_val, join_val
	
def parse(sql_str, current_db=None):
	
	res = sqlglot.parse_one(sql_str)
	print(res)
	print('--------------------------------------')
	
	if res.key == 'create':
		table_name, cols, col_types = create(res)
		new_tbl = current_db.create_table(table_name, cols, col_types)
		return 'Tables:'+str(list(current_db.tables.keys()))

	elif res.key == 'insert':
		table_name, cols, col_vals = insert(res)
		
		ins_tbl = current_db.tables.get(table_name)
		ins_tbl.insert(col_vals, cols)
		ins_tbl.print_table()
		return 'Inserted row into '+ ins_tbl.name

	elif res.key == 'select':
		table_name, cols, where_val, join_val = select(res)
		sel_tbl = current_db.tables.get(table_name) #from
		#sel_tbl.select(cols, where_val, join_val)

		if where_val != None: #where
			where_rows = sel_tbl.where(where_val['operation'],where_val['operand_l'],where_val['operand_r'])
			new_name = sel_tbl.name+'_temp'
			new_tbl = CustomTable(new_name, sel_tbl.columns, sel_tbl.col_types)
			
			new_where_rows = []
			for r in where_rows:
				new_where_rows.append(copy.deepcopy(r.values))
			new_tbl.insert_bulk(new_where_rows, sel_tbl.columns)
			sel_tbl = new_tbl

		#select - cols
		new_name = sel_tbl.name+'_temp'
		new_col_types = []
		col_inds = []
		for c in cols:
			ind = sel_tbl.columns.index(c)
			new_col_types.append(sel_tbl.col_types[ind]) #get [select]column types

			col_inds.append(sel_tbl.columns.index(c)) #get [select]column indices

		new_tbl = CustomTable(new_name, cols, new_col_types)
		
		#col_inds = []
		#for c in cols:
		#	col_inds.append(sel_tbl.columns.index(c))
		
		print('Tables:',list(current_db.tables.keys()))
		first_col = cols[0]
		first_col_keys = list(sel_tbl.col_btrees[first_col].keys())
		for k in first_col_keys:
			res_rows = sel_tbl.col_btrees[first_col].get(k)
			out_rows = []
			print(res_rows)
			for i in range(len(res_rows)):
				new_row = []
				for j in col_inds: #fill in row columns
					new_row.append(copy.deepcopy(res_rows[i].values[j]))

				out_rows.append(new_row)

			#print('After:',out_rows)
			new_tbl.insert_bulk(out_rows, new_tbl.columns)

		new_tbl.print_table()
		return 'Select done'

	elif res.key == 'update':
		table_name, cols, col_vals, where_val = update(res)
		sel_tbl = current_db.tables.get(table_name)  # from
		where_rows = sel_tbl.where(where_val['operation'], where_val['operand_l'], where_val['operand_r'])
		sel_tbl.update(cols, col_vals, where_val)
		return 'Update done'
	elif res.key == 'drop':
		drop_table(res)
	




if __name__ == '__main__':
	#sql = 'CREATE TABLE trips (level INT, row_date INT)'
	sql = 'SELECT name,trips FROM trips WHERE trips = 2.1;'
	#sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) \
	#VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');"
	#sql = 'SELECT OrderID, CustomerName, OrderDate \
	#		FROM Orders \
	#		INNER JOIN Customers ON CustomerID=OrderDate;'
	#sql = 'CREATE DATABASE mydatabase;'
	#sql = 'USE mytbl;'

	#parse(sql)
	#db = DBMS()
	#pickle.dump(, open( 'dbms.pkl', 'wb' ))

	sql_str = 'SELECT name, age FROM school_directory WHERE age >= 10'
	res = sqlglot.parse_one(sql_str)
	select(res)



