from BTrees.OOBTree import OOBTree

from row import Row
import numpy as np
from sqlglot.expressions import DataType

def convert_datatype(val, datatype):
  if datatype == DataType.Type.INT and val != None:
    return int(val)
  if datatype == DataType.Type.DOUBLE and val != None:
    return float(val)
  else:
    return val

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


  def print_table(self, limit=10):
    ret = ''
    count = 0
    for r in self.rows.values():
      if count < limit:
        print(r)
        ret = ret + str(r) +'\n'
      else:
        break
      count += 1
    return ret

  def print_col_b_trees(self):
    for c in self.columns:
      print(c)
      curr_col = self.col_btrees[c]
      for r_lst in curr_col.values():
        if isinstance(r_lst, list):
          for r in r_lst:
            print(r)
        else:
          print(r_lst)
    #return ret

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

    #convert data to types
    count = 0
    for i in range(len(new_row)):
      new_row[i] = convert_datatype(new_row[i], self.col_types[count])
      count += 1

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

  def where(self, where_val):

    if 'OP' in where_val.keys():
      print('hi')
      new_res = [[], []] 

      for i in range(2):
        operator = where_val[str(i)]['operation']
        op_l = where_val[str(i)]['operand_l']
        op_r = where_val[str(i)]['operand_r']
        #print(operator)
        ind = self.columns.index(op_l)
        op_r = convert_datatype(op_r, self.col_types[ind])

        res = None
        if operator == 'eq':
          res = self.col_btrees[op_l].get(op_r)
        elif operator == 'gt':
          res = self.col_btrees[op_l].values(min=op_r,excludemin=True)
        elif operator == 'lt':
          res = self.col_btrees[op_l].values(max=op_r,excludemax=True)
        elif operator == 'gte':
          res = self.col_btrees[op_l].values(min=op_r,excludemin=False)
        elif operator == 'lte':
          res = self.col_btrees[op_l].values(max=op_r,excludemax=False)
        
        #print('WHERE') 
        for r in res:
          if isinstance(r, list):
            for val in r:
              new_res[i].append(val)
          else:
            new_res[i].append(r)

        print(new_res)
      if where_val['OP'] == 'and':
        # print( [value for value in new_res[0] if value in new_res[1]])
        # exit()
        return [value for value in new_res[0] if value in new_res[1]]
        
      elif where_val['OP'] == 'or':
        # print( new_res[0] + new_res[1])
        # exit()
        return list( set(new_res[0]) | set(new_res[1]))
      else:
        print('error in where')
        exit()
      #where_val['']
      #return new_res
    else:
      operator = where_val['operation']
      op_l = where_val['operand_l']
      op_r = where_val['operand_r']

      where_val['']
      ind = self.columns.index(op_l)
      op_r = convert_datatype(op_r, self.col_types[ind])

      res = None
      if operator == 'eq':
        res = self.col_btrees[op_l].get(op_r)
      elif operator == 'gt':
        res = self.col_btrees[op_l].values(min=op_r,excludemin=True)
      elif operator == 'lt':
        res = self.col_btrees[op_l].values(max=op_r,excludemax=True)
      elif operator == 'gte':
        res = self.col_btrees[op_l].values(min=op_r,excludemin=False)
      elif operator == 'lte':
        res = self.col_btrees[op_l].values(max=op_r,excludemax=False)
      
      #print('WHERE') 
      new_res = [] 
      for r in res:
        if isinstance(r, list):
          for val in r:
            new_res.append(val)
        else:
          new_res.append(r)
      #print(r)
      return new_res

  def update(self, cols, new_vals, where_rows):
    #
    res = where_rows
    #
    # print('type of res issssss ', type(res))
    # count = 0
    # if not isinstance(res, list):
    # for i in range(len(res)):
    #   for j in range(len(self.col_types)):
    #     res[i][j] = convert_datatype(res[i], self.col_types[j])
    # else:
    #   for i in range(len(res)):
    #     count = 0
    #     for j in range(len(res[i])):
    #       res[i][j] = convert_datatype(res[i], self.col_types[count])
    #       count += 1

    res_key = []
    for ind in range(len(list(self.rows.values()))):
      for i in range(len(res)):
        if res[i] == list(self.rows.values())[ind]:
          res_key.append(ind)
    orig_vals = []
    col_inds = []
    for i in range(len(res)):
      orig_vals.append(res[i])
    for col in range(len(cols)):
      col_inds.append(list(self.col_btrees.keys()).index(cols[col]))
    for new_ind in range(len(new_vals)):
      new_vals[new_ind] = convert_datatype(new_vals[new_ind], self.col_types[col_inds[new_ind]])

    new_row = []
    for x in range(len(res)):
      temp = []
      new_val_ind = 0
      for i in range(len(res[x].get_vals())):
        if i not in col_inds:
          temp.append(res[x].get_vals()[i])
        else:
          temp.append(new_vals[new_val_ind])
          new_val_ind += 1
      new_row.append(temp)
    row_objs = []
    for i in new_row:
      row_objs.append(Row(i))
    for i in range(len(res_key)):
      del self.rows[res_key[i]]
      self.rows.update({res_key[i]: row_objs[i]})

    new_res = []
    if isinstance(res,list):
      new_res = res
    else:
      new_res.append(res)
    all_updated = []
    for ind in range(len(res)):
      # print('res ind is ', ind)
      cur_row = res[ind]
      up_row = row_objs[ind]
      for c_ind in range(len(self.columns)):
        wants = []
        ups = []
        cur_col = self.columns[c_ind]
        if (cur_col, cur_row.get_vals()[c_ind]) in all_updated:
          continue
        else:
          temps = self.col_btrees[cur_col][cur_row.get_vals()[c_ind]]
          # all_updated.append((cur_col, cur_row.get_vals()[c_ind]))
          if isinstance(temps, list):
            for t in temps:
              if t not in new_res:
                wants.append(t)
              else:
                ups.append(t)
          else:
            if temps not in new_res:
              wants.append(temps)
            else:
              ups.append(temps)
          # print('what we are testing, ' ,cur_col, cur_row.get_vals()[c_ind])
          # print('these are the wants')
          # for w in wants:
          #   print(w.values)
          # print('------------')
          # print('these are the ups')
          # for up in ups:
          #   print(up.values)
          # print('xxxxxxxxxxxx')
        if c_ind in col_inds:
          # print('UPDATED COLUMN')
          ## we want to update this column value
          ## need to fill in both pre_update and update
          all_updated.append((cur_col, cur_row.get_vals()[c_ind]))
          del self.col_btrees[cur_col][cur_row.get_vals()[c_ind]]
          if len(wants) != 0:
            ## we want to keep these in the original
            for w in wants:
              self.col_btrees[cur_col].insert(cur_row.get_vals()[c_ind], w)
            # self.col_btrees[cur_col].update({cur_row.get_vals()[c_ind]: wants})
          ## what is the new value we want to udpate?
          index_of  = col_inds.index(c_ind)
          # print('check index_of is correct:  ', cols[index_of], ' ', self.columns[c_ind])
          ## need to check all res_rows and put in all the correct ones right away
          cor_inds = []
          if len(ups) != 0:
            for up in ups:
              cor_inds.append(new_res.index(up))
          if len(cor_inds) != 0:
            for corind in cor_inds:
              row_res = self.col_btrees[cur_col].get(new_vals[index_of])
              if isinstance(row_res, list):
                row_res.append(row_objs[corind])
              else:
                if row_res != None:
                  row_res = [row_res, row_objs[corind]]
                else:
                  row_res = row_objs[corind]
              # print('if 1 added 0 if not : UPS')
              # if isinstance(row_res, list):
              #   for i in row_res:
              #     print(i.values)
              # else:
              #   if row_res != None:
              #     print(row_res.values)
              if self.col_btrees[cur_col].insert(new_vals[index_of], row_res) == 0:
                del self.col_btrees[cur_col][new_vals[index_of]]
                self.col_btrees[cur_col].insert(new_vals[index_of], row_res)
              # self.col_btrees[cur_col].insert(new_vals[index_of], row_objs[corind])
              # self.col_btrees[cur_col].update({new_vals[index_of]: [row_objs[corind]]})
        else:
          # print('NOT UPDATED COLUMN')

          ## don't want to update
          del self.col_btrees[cur_col][cur_row.get_vals()[c_ind]]
          ## find the ones to keep
          if len(wants) != 0:
            # print('we have wants')
            # print(wants[0])
            ## we want to keep these in the original
            for w in wants:
              # print('if 1 added 0 if not : wants')
              self.col_btrees[cur_col].insert(cur_row.get_vals()[c_ind], w)
            # self.col_btrees[cur_col].update({cur_row.get_vals()[c_ind]: wants})
          ## need to check all res_rows and put in all the correct ones right away
          cor_inds = []
          if len(ups) != 0:
            for up in ups:
              cor_inds.append(new_res.index(up))
          if len(cor_inds) != 0:
            for corind in cor_inds:
              row_res = self.col_btrees[cur_col].get(cur_row.get_vals()[c_ind])
              if isinstance(row_res, list):
                row_res.append(row_objs[corind])
              else:
                if row_res != None:
                  row_res = [row_res, row_objs[corind]]
                else:
                  row_res = row_objs[corind]
              # print('if 1 added 0 if not : UPS')
              # if isinstance(row_res, list):
              #   for i in row_res:
              #     print(i.values)
              # else:
              #   if row_res != None:
              #     print(row_res.values)
              # print('these are keys')
              # print(list(self.col_btrees[cur_col].keys()))
              # print('my key is ', cur_row.get_vals()[c_ind])
              # del self.col_btrees[cur_col][cur_row.get_vals()[c_ind]]
              if self.col_btrees[cur_col].insert(cur_row.get_vals()[c_ind], row_res) == 0:
                del self.col_btrees[cur_col][cur_row.get_vals()[c_ind]]
                self.col_btrees[cur_col].insert(cur_row.get_vals()[c_ind], row_res)
              # self.col_btrees[cur_col].update({cur_row.get_vals()[c_ind]: [row_objs[corind]]})
    return


  def delete(self, where_rows):

    res = where_rows

    res_key = []
    new_res = []
    if isinstance(res,list):
      new_res = res
    else:
      new_res.append(res)
    for ind in range(len(list(self.rows.values()))):
      for i in range(len(res)):
        if res[i] == list(self.rows.values())[ind]:
          res_key.append(ind)
    for i in range(len(res_key)):
      del self.rows[res_key[i]]
    deleted = []
    for ind in range(len(res)):
      cur_row = res[ind]
      for c_ind in range(len(self.columns)):
        wants = []
        cur_col = self.columns[c_ind]
        if (cur_col,cur_row.get_vals()[c_ind]) in deleted:
          continue
        else:
          temps = self.col_btrees[cur_col][cur_row.get_vals()[c_ind]]
          deleted.append((cur_col, cur_row.get_vals()[c_ind]))
          if isinstance(temps, list):
            for t in temps:
              if t not in new_res:
                wants.append(t)
          else:
            if temps not in new_res:
              wants.append(temps)

          ## temps has all entries with name jack

          ## keep the ones we want

          ### lets delete
          del self.col_btrees[cur_col][cur_row.get_vals()[c_ind]]
          if len(wants) != 0:
              self.col_btrees[cur_col].update({cur_row.get_vals()[c_ind]: wants})

  def left_outer_join(self, other_table, op, op_l, op_r):
    outer_tbl_name = self.name +'_outer_'+other_table.name
    outer_tbl_cols = self.columns + other_table.columns
    outer_tbl_col_types = self.col_types +other_table.col_types

    

    inner_tbl_name = self.name +'_outer_'+other_table.name
    inner_tbl_cols = self.columns + other_table.columns
    inner_tbl_col_types = self.col_types +other_table.col_types


    if op_l == op_r:
      col_l = self.columns.index(op_l)
      col_r = other_table.columns.index(op_r)
      outer_tbl_cols[len(self.columns)+col_r] = self.columns[col_r] +'_right'
      inner_tbl_cols[len(self.columns)+col_r] = self.columns[col_r] +'_right'

    outer_tbl = Table(outer_tbl_name,outer_tbl_cols,outer_tbl_col_types)
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
    if op_l == op_r:
      col_l = self.columns.index(op_l)
      col_r = other_table.columns.index(op_r)
      #join_tbl_cols[col_l] = self.columns[col_l] +'_left'
      #op_l = join_tbl_cols[col_l]
      join_tbl_cols[len(self.columns)+col_r] = self.columns[col_r] +'_right'
      #op_r = join_tbl_cols[col_l]
      #print(join_tbl_cols)
      #exit()

    join_tbl_col_types = self.col_types +other_table.col_types
    join_tbl = Table(join_tbl_name,join_tbl_cols,join_tbl_col_types)


    #if op_r in other_table.columns: #compare columns
    #left outer join
    left_keys = list(self.col_btrees[op_l].keys())


    for k in left_keys:
      left_rows = self.col_btrees[op_l].get(k)
      # for ob in left_rows:
      #   print(ob.values)
      # exit()
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


    inner_tbl_name = self.name +'_outer_'+other_table.name
    inner_tbl_cols = self.columns + other_table.columns
    inner_tbl_col_types = self.col_types +other_table.col_types

    if op_l == op_r:
      col_l = self.columns.index(op_l)
      col_r = other_table.columns.index(op_r)
      outer_tbl_cols[col_l] = self.columns[col_l] +'_left'
      inner_tbl_cols[col_l] = self.columns[col_l] +'_left'

      print(inner_tbl_cols)

    outer_tbl = Table(outer_tbl_name,outer_tbl_cols,outer_tbl_col_types)
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


    if op_l == op_r:
      col_l = self.columns.index(op_l)
      col_r = other_table.columns.index(op_r)
      join_tbl_cols[col_l] = self.columns[col_l] +'_left'
      #op_l = join_tbl_cols[col_l]
      #join_tbl_cols[len(self.columns)+col_r] = self.columns[col_r] +'_right'
      #op_r = join_tbl_cols[col_l]
      print(join_tbl_cols)

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
    print(j_type)
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

  def groupby(self, col):
    keys = list(self.col_btrees[col].keys())
    rows = []
    col_ind = self.columns.index(col)
    if self.col_types[col_ind] == 'int':
        keys.sort(key=int)
    else:
        keys.sort()
    for k in keys:
      rows.append(self.col_btrees[col].get(k))
    grp_tables = []
    for row in range(len(rows)):
      temp_table = Table(col +' '+ keys[row],self.columns,self.col_types)
      for j in rows[row]:
        temp_table.insert(j.values, self.columns)
      grp_tables.append(temp_table)
    return grp_tables


  def orderby(self, col, desc=None):
    order_table = Table(self.name+'_ordby_'+col,self.columns,self.col_types)
    keys = list(self.col_btrees[col].keys())
    rows = []
    col_ind = self.columns.index(col)
    if self.col_types[col_ind] == 'int':
      if desc == True:
        keys.sort(key=int, reverse=True)
      else:
        keys.sort(key=int)
    else:
      if desc == True:
        keys.sort(reverse=True)
      else:
        keys.sort()
    for k in keys:
      rows.append(self.col_btrees[col].get(k))
    for row in rows:
      for i in row:
        order_table.insert(i.values, self.columns)
    return order_table


  def max(self, col):
    cur_col = self.col_btrees[col]
    keys = list(cur_col.keys())
    col_ind = self.columns.index(col)
    if self.col_types[col_ind] == 'int':
      new_keys = [int(x) for x in keys]
    else:
      new_keys = keys
    max_key = max(new_keys)
    max_ind = new_keys.index(max_key)
    act_max = keys[max_ind]
    row = self.col_btrees[col].get(act_max)
    return row[0]


  def min(self,col):
    cur_col = self.col_btrees[col]
    keys = list(cur_col.keys())
    col_ind = self.columns.index(col)
    if self.col_types[col_ind] == 'int':
      new_keys = [int(x) for x in keys]
    else:
      new_keys = keys
    min_key = min(new_keys)
    min_ind = new_keys.index(min_key)
    act_min = keys[min_ind]
    row = self.col_btrees[col].get(act_min)
    return row[0]

  def avg(self, col):
    col_ind = self.columns.index(col)
    lst = []
    for r in self.rows.values():
      lst.append(r.get_vals()[col_ind])
    if self.col_types[col_ind] == 'int':
      new_list = [int(x) for x in lst]
    else:
      new_list = lst
    return sum(new_list) / len(new_list)

  def count(self, col):
    col_ind = self.columns.index(col)
    lst = []
    for r in self.rows.values():
      lst.append(r.get_vals()[col_ind])
    if self.col_types[col_ind] == 'int':
      new_list = [int(x) for x in lst]
    else:
      new_list = lst
    return len(new_list)

  def sum(self, col):
    col_ind = self.columns.index(col)
    lst = []
    for r in self.rows.values():
      lst.append(r.get_vals()[col_ind])
    if self.col_types[col_ind] == 'int':
      new_list = [int(x) for x in lst]
    else:
      new_list = lst
    return sum(new_list)
