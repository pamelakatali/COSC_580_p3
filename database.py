from BTrees.OOBTree import OOBTree

from table import Table

class DB:
  def __init__(self, name):
    self.name = name
    self.tables = OOBTree() #[]


  def create_table(self, table_name, columns, col_types):
    new_table = Table(table_name, columns, col_types)
    self.tables.update({table_name: new_table})
    print('Tables:',list(self.tables.keys()))
    return new_table 

  def drop_table(self,table_name):
    print(list(self.tables))
    del self.tables[table_name]
    #self.tables.__delitem__(table)
    print(list(self.tables))