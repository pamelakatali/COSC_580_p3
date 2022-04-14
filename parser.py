import sqlglot
from sqlglot.expressions import ColumnDef, Identifier, DataType, From, Star, Table, Values, Literal, Tuple, Where, EQ, Column
from BTrees.OOBTree import OOBTree


#print(res.__repr__)


#CREATE TABLE - if res.key == 'create'
def create(res):
	res = res
	res_gen = res.find_all(ColumnDef)
	table_name = res.find(Table).args['this'].args['this']
	cols = []
	col_typs = []
	for comm in res_gen:
		idt = comm.find(Identifier)
		col_name = idt.args['this']
		dtyp = comm.find(DataType)
		col_type = dtyp.args['this']

		cols.append(col_name)
		col_typs.append(col_type)
	print('Table name:', table_name)
	print('Columns:',cols)
	print('Column types:',col_typs)

## UPDATE this if res.key == 'update'
def update(res):
    table_name = res.find(Table).args['this'].args['this']

    expr = res.args['expressions']
    cols = []
    where = res.args['where']
    func = where.args['this'].key
    where_col = where.args['this'].args['this'].args['this'].args['this']
    where_val = where.args['this'].args['expression'].args['this']
    where_id = [func, where_col, where_val]
    col_vals = []

    if len(expr) == 1:
        cols = [expr.find(Identifier).args['this']]
    else:
        for col in expr:
            cols.append(col.find(Identifier).args['this'])
            col_vals.append(col.find(Literal).args['this'])

    print(where_id)
    print(cols)
    print(col_vals)

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
	if res['joins'] != None:
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
	
	

if __name__ == '__main__':
	sql = 'CREATE TABLE trips (level INT, row_date INT);'
	#sql = 'SELECT name,trips FROM trips WHERE trips = 2.1;'
	#sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) \
	#VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');"
	sql = 'SELECT OrderID, CustomerName, OrderDate \
			FROM Orders \
			INNER JOIN Customers ON CustomerID=OrderDate;'

	res = sqlglot.parse_one(sql)
	print(res)
	print('--------------------------------------')

	if res.key == 'create':
		create(res)
	elif res.key == 'insert':
		insert(res)
	elif res.key == 'select':
		select(res)
	elif res.key == 'update':
		update(res)
	elif res.key == 'drop':
		drop_table(res)
