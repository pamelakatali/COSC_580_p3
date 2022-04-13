from BTrees.OOBTree import OOBTree

class DBMS:
  def __init__(self):
    self.databases = OOBTree() #[]

  def create_db(self, db_name):
    new_db = DB(db_name)
    self.databases.update({db_name: new_db})

    print('Databases:',list(self.databases.keys()))
    return new_db


class DB:
  def __init__(self, name):
    self.name = name
    self.tables = OOBTree() #[]

  def create_table(self, table_name, columns, col_types):
    new_table = Table(table_name, columns, col_types)
    self.tables.update({table_name: new_table})
    print('Tables:',list(self.tables.keys()))
    return new_table

class Row:
  def __init__(self, values):
    self.values = values

  def __len__(self):
    return len(self.values)


  def __eq__(self, other):
    for i in range(len(self.values)):
      if self.values[i] != other.values[i]:
        return False
    return True


class Table:
  def __init__(self, name, columns, col_types, primary_key=None):
    self.columns = columns
    self.col_types = col_types
    self.rows = OOBTree() #[] #store as b-tree based on primary key
    
    self.col_btrees = {}
    for c in columns:
      self.col_btrees[c] = OOBTree() #key, lst of pointers

    #print(self.col_btrees)
    #self.primary_key = primary_key
    #self.foreign_key = None

  def insert(self, new_row, row_cols):
    #insert into b-tree
    ind = len(self.rows)
    row_obj = Row(new_row)
    self.rows.update({ind:row_obj})

    for i in range(len(new_row)):
      curr_col = self.columns[i]
      if self.col_btrees[curr_col].get(new_row[i]) == None:
        self.col_btrees[curr_col].update({new_row[i]:[row_obj]})
      else:
        output = self.col_btrees[curr_col].get(new_row[i])
        output.append(row_obj)
        self.col_btrees[curr_col].update({new_row[i]:output})

    print('Insert:',list(self.rows.keys()), list(self.rows.values()))
    print(list(self.col_btrees['name'].keys()))
    print(list(self.col_btrees['name'].values()))
    print(list(self.col_btrees['grade'].keys()))

#  def where(self, operator, op_l, op_r):
#    if operator == 'eq':
#      self.rows.iterkeys


  #def update(self, curr_row):
  #def delete(self, curr_row):


