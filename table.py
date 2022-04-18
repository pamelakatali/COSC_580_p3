from BTrees.OOBTree import OOBTree

from row import Row
import numpy as np

class Table:
  def __init__(self, name, columns, col_types, primary_key=None):
    self.name = name
    self.columns = columns
    self.col_types = col_types
    self.rows = OOBTree() #[] #store as b-tree based on primary key
    
    self.col_btrees = {} #create btree to index all objects
    for c in columns:
      self.col_btrees[c] = OOBTree() #key, lst of pointers

    #print(self.col_btrees)
    #self.primary_key = primary_key
    #self.foreign_key = None


  def print_table(self):
    for r in self.rows.values():
      print(r)

  def combine_tables(self, other_table):
    for k in other_table.rows.keys():
      new_row = other_table.rows.get(k)
      self.insert(new_row.values,self.columns)

  def insert_bulk(self, new_rows,row_cols):
    for row in new_rows:
      self.insert(row, row_cols)

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

  def where(self, operator, op_l, op_r):
    res = None
    if operator == 'eq':
      res = self.col_btrees[op_l].get(op_r)
    
    #print('WHERE')  
    #for r in res:
    #  print(r)
    return res

  def update(self, cols, new_vals, where_ids):
    func = where_ids[0]
    where_col = where_ids[1]
    where_val = where_ids[2]
    res = None

    if func == 'eq':
      res = self.col_btrees[where_col].get(where_val)
    res_key = None
    for ind in range(len(list(self.rows.values()))):
      if res[0] == list(self.rows.values())[ind]:
        res_key = ind
    col_inds = []
    for col in range(len(cols)):
      col_inds.append(list(self.col_btrees.keys()).index(cols[col]))
    new_row =[]
    for i in range(len(res[0].get_vals())):
      if i not in col_inds:
        new_row.append(res[0].get_vals()[i])
      else:
        new_row.append(new_vals[i])
    row_obj = Row(new_row)
    del self.rows[res_key]
    self.rows.update({res_key:row_obj})

  def left_outer_join(self, other_table, op, op_l, op_r):
    outer_tbl_name = self.name +'_outer_'+other_table.name
    outer_tbl_cols = self.columns + other_table.columns
    outer_tbl_col_types = self.col_types +other_table.col_types
    outer_tbl = Table(outer_tbl_name,outer_tbl_cols,outer_tbl_col_types)

    inner_tbl_name = self.name +'_outer_'+other_table.name
    inner_tbl_cols = self.columns + other_table.columns
    inner_tbl_col_types = self.col_types +other_table.col_types
    inner_tbl = Table(inner_tbl_name,inner_tbl_cols,inner_tbl_col_types)
    
    left_keys = list(self.col_btrees[op_l].keys())

    for k in left_keys:
      left_rows = self.col_btrees[op_l].get(k)
      right_rows = other_table.col_btrees[op_r].get(k) #lst 

      for lft_row in left_rows:
        if right_rows != None:
          for rght_row in right_rows:
            join_rw = lft_row.values + rght_row.values
            inner_tbl.insert(join_rw, inner_tbl_cols)
        else:
          rght_row = [None]*len(other_table.columns)
          join_rw = lft_row.values + rght_row
          outer_tbl.insert(join_rw, outer_tbl_cols)
            
    return outer_tbl, inner_tbl  


  def left_join(self, other_table, op, op_l, op_r):
    join_tbl_name = self.name +'_'+other_table.name
    join_tbl_cols = self.columns + other_table.columns
    join_tbl_col_types = self.col_types +other_table.col_types
    join_tbl = Table(join_tbl_name,join_tbl_cols,join_tbl_col_types)

    #if op_r in other_table.columns: #compare columns
    #left outer join
    left_keys = list(self.col_btrees[op_l].keys())
    for k in left_keys:
      left_rows = self.col_btrees[op_l].get(k)
      right_rows = other_table.col_btrees[op_r].get(k) #lst

      for lft_row in left_rows:
        if right_rows != None:
          for rght_row in right_rows:
            join_rw = lft_row.values + rght_row.values
            join_tbl.insert(join_rw, join_tbl_cols)
        else:
          rght_row = [None]*len(other_table.columns)
          join_rw = lft_row.values + rght_row
          join_tbl.insert(join_rw, join_tbl_cols)
            
    return join_tbl

  def right_outer_join(self, other_table, op, op_l, op_r):
    outer_tbl_name = self.name +'_outer_'+other_table.name
    outer_tbl_cols = self.columns + other_table.columns
    outer_tbl_col_types = self.col_types +other_table.col_types
    outer_tbl = Table(outer_tbl_name,outer_tbl_cols,outer_tbl_col_types)

    inner_tbl_name = self.name +'_outer_'+other_table.name
    inner_tbl_cols = self.columns + other_table.columns
    inner_tbl_col_types = self.col_types +other_table.col_types
    inner_tbl = Table(inner_tbl_name,inner_tbl_cols,inner_tbl_col_types)

    right_keys = list(other_table.col_btrees[op_r].keys())
    for k in right_keys:
      left_rows = self.col_btrees[op_l].get(k)
      right_rows = other_table.col_btrees[op_r].get(k) #lst

      for rght_row in right_rows:
        if left_rows != None:
            for lft_row in left_rows:
              join_rw = lft_row.values + rght_row.values
              inner_tbl.insert(join_rw, inner_tbl_cols)
        else:
          lft_row = [None]*len(self.columns)
          join_rw = lft_row + rght_row.values
          outer_tbl.insert(join_rw, outer_tbl_cols)


    return outer_tbl, inner_tbl

  def right_join(self, other_table, op, op_l, op_r):
    join_tbl_name = self.name +'_'+other_table.name
    join_tbl_cols = self.columns + other_table.columns
    join_tbl_col_types = self.col_types +other_table.col_types
    join_tbl = Table(join_tbl_name,join_tbl_cols,join_tbl_col_types)


    right_keys = list(other_table.col_btrees[op_r].keys())
    for k in right_keys:
      left_rows = self.col_btrees[op_l].get(k)
      right_rows = other_table.col_btrees[op_r].get(k) #lst

      for rght_row in right_rows:
        if left_rows != None:
            for lft_row in left_rows:
              join_rw = lft_row.values + rght_row.values
              join_tbl.insert(join_rw, join_tbl_cols)
        else:
          lft_row = [None]*len(self.columns)
          join_rw = lft_row + rght_row.values
          join_tbl.insert(join_rw, join_tbl_cols)

    return join_tbl

  def join(self, other_table, op, op_l, op_r, j_type='left'):
    if j_type == 'left':
      return self.left_join(other_table, op, op_l, op_r)
    elif j_type == 'right':
      return self.right_join(other_table, op, op_l, op_r)
    elif j_type == 'inner':
      if len(self.rows) >= len(other_table.rows):
        _ , inner_tbl = self.left_outer_join(other_table, op, op_l, op_r)
      else:
        _ , inner_tbl = self.right_outer_join(other_table, op, op_l, op_r)
      
      return inner_tbl
    else: #cross join/full outer
      left_join_tbl = self.left_join(other_table, op, op_l, op_r)
      right_outer_tbl, _ = self.right_outer_join(other_table, op, op_l, op_r)

      left_join_tbl.combine_tables(right_outer_tbl)

      return left_join_tbl      

