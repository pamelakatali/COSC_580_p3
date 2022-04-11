import sqlglot
from sqlglot.expressions import ColumnDef, Identifier, DataType, From, Star, Table, Values, Literal, Tuple
from BTrees.OOBTree import OOBTree


sql = 'CREATE TABLE trips (level VARCHAR(30), row_date DATE);'
sql = 'SELECT name,people FROM trips WHERE name = people;'
sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) \
VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');"

res = sqlglot.parse_one(sql)
print(res)
print('--------------------------------------')


#print(res.__repr__)


#CREATE TABLE - if res.key == 'create'
'''
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
'''

#INSERT INTO - add row to table
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


#SELECT FROM - if res.key == 'select'
'''
res = res.args
print(res.keys())

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
'''

