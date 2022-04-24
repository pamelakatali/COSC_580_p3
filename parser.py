import sqlglot
from sqlglot.expressions import ColumnDef, Identifier, DataType, From, Star, Table, Values, Literal, Tuple, Where, EQ, \
	Column, Min, Max, Avg, Sum, Count, Ordered, Group, Order
from BTrees.OOBTree import OOBTree
import pickle

from table import Table as CustomTable
import copy


# CREATE TABLE - if res.key == 'create'
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
	print('Columns:', cols)
	print('Column types:', col_types)

	return table_name, cols, col_types


## UPDATE this if res.key == 'update'
def update(res):
	table_name = res.find(Table).args['this'].args['this']
	expr = res.args['expressions']
	cols = []
	wheres = res.args['where']
	col_vals = []
	where_val = None
	lit_flag = False
	if expr[0].find(Literal) == None:
		lit_flag = False
	else:
		lit_flag = True

	if wheres != None:
		where_val = where(wheres)
	if len(expr) == 1:
		cols = [expr[0].args['this'].args['this'].args['this']]
		if lit_flag == False:
			col_vals = [expr[0].args['expression'].args['this'].args['this']]
		else:
			col_vals = [expr[0].find(Literal).args['this']]
	else:
		for col in expr:
			cols.append(col.args['this'].args['this'].args['this'])
			if lit_flag == False:
				col_vals.append(col.args['expression'].args['this'].args['this'])
			else:
				col_vals.append(col.find(Literal).args['this'])
	return table_name, cols, col_vals, where_val


# DROP Table
def drop_table(res):
	table_name = res.find(Table).args['this'].args['this']
	print('Delete table: ', table_name)
	return (table_name)


# INSERT INTO - add row to table
def insert(res):
	# print(res)
	table_name = res.find(Table).args['this'].args['this']
	# print(res.args.keys())
	# print(res.args['expression'].find(Tuple).args['expressions'])  # .args['expressions'])

	cols_gen = res.args['this'].args['expressions']
	vals_gen = res.args['expression'].find(Tuple).args['expressions']
	cols = []
	vals = []

	for col in cols_gen:
		cols.append(col.args['this'])

	for val in vals_gen:
		vals.append(val.args['this'])

	print('Table name:', table_name)
	print('Columns:', cols)
	print('Column values:', vals)

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
	# print(res.args)
	if res.args['kind'] != None:
		join_dict['type'] = res.args['kind'].lower()  # inner, cross
	else:
		join_dict['type'] = res.args['side'].lower()  # left, right

	join_dict['Table'] = res.find(Table).args['this'].args['this']

	res = res.args['on']
	join_dict['operation'] = res.key

	join_dict['operand_l'] = res.args['this'].args['this'].args['this']
	if res.args['expression'].find(Column) == None:
		join_dict['operand_r'] = res.args['expression'].args['this']
	else:
		join_dict['operand_r'] = res.args['expression'].args['this'].args['this']

	# print(join_dict)
	return join_dict


def orderby(res):
	col_name = res.find(Identifier).args['this']
	key = res.find(Ordered).args['desc']
	return [col_name, key]


def groupby(res):
	group_col = res.find(Identifier).args['this']
	return group_col


def find_pre(res):
	pre = None
	if res.find(Min) != None:
		pre = 'Min'
	elif res.find(Max) != None:
		pre = 'Max'
	elif res.find(Sum) != None:
		pre = 'Sum'
	elif res.find(Avg) != None:
		pre = 'Avg'
	elif res.find(Count) != None:
		pre = 'Count'
	else:
		pre = None
	return pre


def delete(res):
	table_name = res.find(Table).args['this'].args['this']
	cols = []
	wheres = res.args['where']
	col_vals = []
	where_val = None
	if wheres != None:
		where_val = where(wheres)
	return table_name, where_val


def limit(res):
	return res.args['this'].args['this']


def select(res):
	res = res.args

	join_val = None
	# print(res['joins'])
	if len(res['joins']) > 0:
		join_val = join(res['joins'])

	where_val = None
	if res['where'] != None:
		where_val = where(res['where'])

	order_col = None
	if res['order'] != None:
		order_col = orderby(res['order'])

	group_col = None
	if res['group'] != None:
		group_col = groupby(res['group'])

	limit_val = None
	if res['limit'] != None:
		limit_val = limit(res['limit'])
		limit_val = int(limit_val)

	table_name = res['from'].args['expressions'][0].args['this'].args['this']

	expr = res['expressions']
	cols = []
	pres = []
	if len(expr) == 1:
		expr = expr[0]
		is_star = expr.find(Star)
		if is_star is not None:
			cols = ['star']
		else:
			pres = [find_pre(expr)]
			cols = [expr.find(Identifier).args['this']]
	else:
		for col in expr:
			pres.append(find_pre(col))
			cols.append(col.find(Identifier).args['this'])

	print('Table name:', table_name)
	print('Prefixes:', pres)
	print('Columns:', cols)
	print('Where:', where_val)
	print('Join:', join_val)
	print('Order_by:', order_col)
	print('Group_by:', group_col)
	print('Limit:', limit_val)

	return table_name, pres, cols, where_val, join_val, order_col, group_col, limit_val


def pre_sel(cur_table, pres, cols, col_types, col_inds):
	pre_inds = []
	non_pre = []
	first_pre_ind = None
	for i in range(len(pres)):
		if pres[i] != None:
			pre_inds.append(i)
		else:
			non_pre.append(i)
	res_rows = []
	new_cols = []
	new_new_col_types = []
	temp_rows = []
	for pre_ind in pre_inds:
		res_row = None
		pre = pres[pre_ind]
		if pre == 'Min':
			if first_pre_ind == None:
				first_pre_ind = pre_ind
			new_cols.append(cols[pre_ind])
			res_row = cur_table.min(cols[pre_ind]).get_vals()[col_inds[pre_ind]]
			temp_rows.append(cur_table.min(cols[pre_ind]))
			new_new_col_types.append(col_types[pre_ind])
		elif pre == 'Max':
			if first_pre_ind == None:
				first_pre_ind = pre_ind
			new_cols.append(cols[pre_ind])
			res_row = cur_table.max(cols[pre_ind]).get_vals()[col_inds[pre_ind]]
			new_new_col_types.append(col_types[pre_ind])
			temp_rows.append(cur_table.max(cols[pre_ind]))
		elif pre == 'Sum':
			new_cols.append(cols[pre_ind])
			res_row = cur_table.sum(cols[pre_ind])
			new_new_col_types.append(col_types[pre_ind])
		elif pre == 'Count':
			new_cols.append(cols[pre_ind])
			res_row = cur_table.count(cols[pre_ind])
			new_new_col_types.append(col_types[pre_ind])
		elif pre == 'Avg':
			new_cols.append(cols[pre_ind])
			res_row = cur_table.avg(cols[pre_ind])
			new_new_col_types.append(col_types[pre_ind])
		res_rows.append(res_row)
	for non_ind in non_pre:
		new_cols.insert(non_ind, cols[non_ind])
		new_new_col_types.append(col_types[non_ind])
	first_row = None
	if first_pre_ind != None:
		first_row = temp_rows[0]
	row_vals = []

	for i in range(len(new_cols)):
		if i not in pre_inds:
			col_name = new_cols[i]
			cur_ind = cur_table.columns.index(col_name)
			if first_row != None:
				row_vals.append(first_row.get_vals()[cur_ind])
			else:
				row_vals.append(cur_table.rows[0].get_vals()[cur_ind])
		else:
			row_vals.append(res_rows[i])
	return row_vals, new_cols, new_new_col_types


def make_rel_table(rel_name, cur_db):
	new_tbl = None
	col_types = [DataType.Type.INT, DataType.Type.INT]
	if rel_name == 'rel_i_i_1000':
		table_name = 'rel_i_i_1000'
		columns = ['ii10001', 'ii10002']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_i_1000_rows = new_tbl.rel_1()
		new_tbl.insert_bulk(rel_i_i_1000_rows, columns)
	elif rel_name == 'rel_i_1_1000':
		table_name = 'rel_i_1_1000'
		columns = ['i110001', 'i110002']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_1_1000_rows = new_tbl.rel_2()
		new_tbl.insert_bulk(rel_i_1_1000_rows, columns)
	elif rel_name == 'rel_i_i_10000':
		table_name = 'rel_i_i_10000'
		columns = ['ii100001', 'ii100001']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_i_10000_rows = new_tbl.rel_3()
		new_tbl.insert_bulk(rel_i_i_10000_rows, columns)
	elif rel_name == 'rel_i_1_10000':
		table_name = 'rel_i_1_10000'
		columns = ['i1100001', 'i1100001']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_1_10000_rows = new_tbl.rel_4()
		new_tbl.insert_bulk(rel_i_1_10000_rows, columns)
	elif rel_name == 'rel_i_i_100000':
		table_name = 'rel_i_i_100000'
		columns = ['ii1000001', 'ii1000002']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_i_100000_rows = new_tbl.rel_3()
		new_tbl.insert_bulk(rel_i_i_100000_rows, columns)
	elif rel_name == 'rel_i_1_100000':
		table_name = 'rel_i_1_100000'
		columns = ['i11000001', 'i11000002']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_1_100000_rows = new_tbl.rel_4()
		new_tbl.insert_bulk(rel_i_1_100000_rows, columns)
	elif rel_name == 'rel_i_i_10':
		table_name = 'rel_i_i_10'
		columns = ['ii101','ii102']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_i_10_rows = new_tbl.rel_x()
		new_tbl.insert_bulk(rel_i_i_10_rows, columns)
	elif rel_name == 'rel_i_1_10':
		table_name = 'rel_i_1_10'
		columns = ['i1101', 'i1102']
		new_tbl = cur_db.create_table(table_name, columns, col_types)
		rel_i_1_10_rows = new_tbl.rel_y()
		new_tbl.insert_bulk(rel_i_1_10_rows, columns)
	return new_tbl

pre_loads = ['rel_i_i_10', 'rel_i_1_10','rel_i_i_1000','rel_i_1_1000','rel_i_i_10000','rel_i_1_10000','rel_i_i_100000','rel_i_1_100000']

def parse(sql_str, current_db=None):

	if sql_str in pre_loads:
		new_tbl = make_rel_table(sql_str, current_db)
		return 'Tables: ' + str(list(current_db.tables.keys()))

	res = sqlglot.parse_one(sql_str)
	# print(res)
	print('--------------------------------------')

	if res.key == 'create':
		table_name, cols, col_types = create(res)
		new_tbl = current_db.create_table(table_name, cols, col_types)
		return 'Tables:' + str(list(current_db.tables.keys()))

	elif res.key == 'insert':
		table_name, cols, col_vals = insert(res)

		ins_tbl = current_db.tables.get(table_name)
		ins_tbl.insert(col_vals, cols)
		ins_tbl.print_table()
		return 'Inserted row into ' + ins_tbl.name

	elif res.key == 'select':
		table_name, pres, cols, where_val, join_val, order_col, group_col, limit_val = select(res)

		sel_tbl = current_db.tables.get(table_name)  # from

		# sel_tbl.select(cols, where_val, join_val)

		if join_val != None:
			other_table = current_db.tables.get(join_val['Table'])
			sel_tbl = sel_tbl.join(other_table, join_val['operation'], join_val['operand_l'], join_val['operand_r'],
								   join_val['type'])

		if where_val != None:  # where
			where_rows = sel_tbl.where(where_val['operation'], where_val['operand_l'], where_val['operand_r'])
			new_name = sel_tbl.name + '_temp'
			new_tbl = CustomTable(new_name, sel_tbl.columns, sel_tbl.col_types)

			new_where_rows = []
			for r in where_rows:
				new_where_rows.append(copy.deepcopy(r.values))
			new_tbl.insert_bulk(new_where_rows, sel_tbl.columns)
			sel_tbl = new_tbl

		# select - cols
		new_name = sel_tbl.name + '_temp'
		new_col_types = []
		if cols[0] == 'star':
			cols = sel_tbl.columns
		col_inds = []
		for c in cols:
			ind = sel_tbl.columns.index(c)
			new_col_types.append(sel_tbl.col_types[ind])  # get [select]column types
			col_inds.append(sel_tbl.columns.index(c))  # get [select]column indices
		new_tbl = CustomTable(new_name, cols, new_col_types)

		# col_inds = []
		# for c in cols:
		#	col_inds.append(sel_tbl.columns.index(c))

		print('Tables:', list(current_db.tables.keys()))
		grp_tables = None
		if group_col != None:
			grp_tables = sel_tbl.groupby(group_col)
			cur_table = grp_tables[0]
			for ind in range(len(grp_tables) - 1):
				cur_table.insert(grp_tables[0].rows[0].values, grp_tables[0].columns)
		first_col = cols[0]

		first_col_keys = list(sel_tbl.col_btrees[first_col].keys())

		if 'Min' in pres or 'Max' in pres or 'Sum' in pres or 'Count' in pres or 'Avg' in pres:
			if group_col == None:
				row_vals, new_cols, new_new_col_types = pre_sel(sel_tbl, pres, cols, new_col_types, col_inds)
				new_tbl = CustomTable(new_name, new_cols, new_new_col_types)
				new_tbl.insert(row_vals, new_cols)
			else:
				grp_rows = []
				new_cols = None
				new_new_col_types = None
				for tbl in grp_tables:
					row_vals, new_cols, new_new_col_types = pre_sel(tbl, pres, cols, new_col_types, col_inds)
					grp_rows.append(row_vals)
				new_tbl = CustomTable(new_name, new_cols, new_new_col_types)
				for vals in grp_rows:
					new_tbl.insert(vals, new_cols)
		else:
			for k in first_col_keys:
				res_rows = sel_tbl.col_btrees[first_col].get(k)
				if not isinstance(res_rows, list):
					res_rows = [res_rows]
				out_rows = []
				for i in range(len(res_rows)):
					new_row = []
					for j in col_inds:  # fill in row columns
						new_row.append(copy.deepcopy(res_rows[i].values[j]))

					out_rows.append(new_row)
				# print('After:',out_rows)
				new_tbl.insert_bulk(out_rows, new_tbl.columns)

		if order_col != None:
			new_tbl = new_tbl.orderby(order_col[0], order_col[1])

		print('THIS IS THE NEW TABLE')
		print(new_tbl.columns)
		if limit_val != None:
			new_tbl.print_table(limit=limit_val)
			return new_tbl.print_table(limit=limit_val)
		else:
			new_tbl.print_table()

		return new_tbl.print_table()  # 'Select done'

	elif res.key == 'update':
		table_name, cols, col_vals, where_val = update(res)
		sel_tbl = current_db.tables.get(table_name)  # from
		where_rows = sel_tbl.where(where_val['operation'], where_val['operand_l'], where_val['operand_r'])
		sel_tbl.update(cols, col_vals, where_rows)
		return sel_tbl.print_table()
	elif res.key == 'drop':
		table_name = drop_table(res)
		current_db.drop_table(table_name)
		return table_name + ' dropped'
	elif res.key == 'delete':
		table_name, where_val = delete(res)
		sel_tbl = current_db.tables.get(table_name)
		where_rows = sel_tbl.where(where_val['operation'], where_val['operand_l'], where_val['operand_r'])
		sel_tbl.delete(where_rows)
		return 'deleted'


if __name__ == '__main__':
	# sql = 'CREATE TABLE trips (level INT, row_date INT)'
	sql = 'SELECT name,trips FROM trips WHERE trips = 2.1;'
	# sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) \
	# VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');"
	# sql = 'SELECT OrderID, CustomerName, OrderDate \
	#		FROM Orders \
	#		INNER JOIN Customers ON CustomerID=OrderDate;'
	# sql = 'CREATE DATABASE mydatabase;'
	# sql = 'USE mytbl;'

	# parse(sql)
	# db = DBMS()
	# pickle.dump(, open( 'dbms.pkl', 'wb' ))

	# sql_str = "INSERT INTO school_directory (name, age, grade) VALUES ('jack', 8, 2)"
	# res = sqlglot.parse_one(sql_str)
	# insert(res)

	sql_str2 = 'SELECT MIN(age), name FROM school_directory ORDER BY grade DESC'
	sql_str2 = "SELECT * FROM school_directory LIMIT 10"
	sql_str2 = "UPDATE school_directory SET age = 12 WHERE name = 'jane'"
	sql_str2 = "SELECT name, age, grade, height FROM school_directory RIGHT JOIN height_tbl ON name = name"
	res = sqlglot.parse_one(sql_str2)

	select(res)
# print(res.args['order'].find(Ordered).args['desc'])
# print(cols)