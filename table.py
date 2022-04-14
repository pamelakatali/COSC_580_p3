from BTrees.OOBTree import OOBTree

from row import Row

class Table:
  def __init__(self, name, columns, col_types, primary_key=None):
    self.columns = columns
    self.col_types = col_types
    self.rows = OOBTree() #[] #store as b-tree based on primary key
    
    self.col_btrees = {} #create btree to index all objects
    for c in columns:
      self.col_btrees[c] = OOBTree() #key, lst of pointers

    #print(self.col_btrees)
    #self.primary_key = primary_key
    #self.foreign_key = None

  def insert(self, new_row, row_cols):
    #insert into b-tree
    ind = len(self.rows) #automatically generated ID
    row_obj = Row(new_row) #create new row obj with row values
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

  def where(self, operator, op_l, op_r):
    res = None
    if operator == 'eq':
      res = self.col_btrees[op_l].get(op_r)
    
    print('WHERE')  
    for r in res:
      print(r)
    return res

    def join(self, other_table, op, op_l, op_r):
      join_tbl_name = self.name +'_'+other_table.name
      join_tbl_cols = self.columns + other_table.columns
      join_tbl_col_types = self.col_types +other_table.col_types
      join_tbl = Table(join_tbl_name,join_tbl_cols,join_tbl_col_types)

      if op_r in other_table.values: #compare columns
        #left outer join
        left_keys = list(self.col_btrees[op_l].keys())
        for k in left_keys:
          left_rows = self.col_btrees[op_l].get(k)
          right_rows = other_table.col_btrees[op_r].get(k) #lst

          for lft_rw in left_rows:
            if right_rows != None:
              for rght_row in right_rows:
                join_rw = lft_rw + rght_rw
                join_tbl.insert(join_rw, join_tbl_cols)
            else:
              rght_rw = [None]*len(other_table.columns)
              join_rw = lft_rw + rght_rw
              join_tbl.insert(join_rw, join_tbl_cols)
              
        return join_tbl




  #def update(self, curr_row):
  #def delete(self, curr_row):


