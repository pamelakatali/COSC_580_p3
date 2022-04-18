from BTrees.OOBTree import OOBTree

from database import DB

class DBMS:
  def __init__(self):
    self.databases = OOBTree() #dbs stored in OOBTree for easy access
    self.curr_db = None

  def create_db(self, db_name):
    new_db = DB(db_name)
    self.databases.update({db_name: new_db}) #add new db to tree

    print('Databases:',list(self.databases.keys()))
    return new_db

