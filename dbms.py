from BTrees.OOBTree import OOBTree

class DBMS:
  def __init__(self, t):
    self.databases = []

  def create_db(self, db_name):
    new_db = DB(db_name)
    self.databases.append(new_db)


class DB:
  def __init__(self, name):
    self.name = name
    self.tables = []

  def create_table(self, table_name, columns, col_types):
    new_table = Table(table_name)
    self.tables.append(new_table)


class Table:
  def __init__(self, name, columns, col_types, primary_key=None):
    self.columns = columns
    self.col_types = col_types
    self.rows = OOBTree #[] #store as b-tree based on primary key
    #self.primary_key = primary_key
    #self.foreign_key = None

  def insert(self, new_row, row_cols):
    #insert into b-tree
    ind = len(self.t)
    self.rows.update({ind:new_row})

  def update(self, curr_row):
    

  def delete(self, curr_row):
    #self.rows.remove(curr_row)


