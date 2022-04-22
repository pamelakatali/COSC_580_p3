from flask import Flask, request, jsonify
from flask_cors import CORS
from parser import parse

import sqlglot
import pickle

from dbms import DBMS
from database import DB
from table import Table

#Set up Flask:
app = Flask(__name__)

#Set up Flask to bypass CORS:
cors = CORS(app)

#set up dbms 
dbms = DBMS()
#pickle.dump(db, open( 'dbms.pkl', 'wb' ))

'''
#Create the receiver API POST endpoint:
@app.route("/receiver", methods=["POST"])
def postME():
 data = request.get_json()
 data = jsonify(data)

 data = sqlglot.parse_one(data)
 return data
'''

@app.route('/foo')
def index():
    return request.base_url 

@app.route('/pass_val',methods=['POST'])
def pass_val():
    sql_str = request.args.get('value')
    #parsed_sql = sqlglot.parse_one(sql_str)
    vals = sql_str.split(' ')
    if len(dbms.databases) == 0: #create db if none
        if vals[0].lower() == 'create':
            vals = sql_str.split(' ')
            db = dbms.create_db(vals[2]) 
            res = 'Databases:'+str(list(dbms.databases))
        else:
            res = 'CREATE a database to implement actions'
    elif dbms.curr_db is None: #create table
        if vals[0].lower() == 'use':
            vals = sql_str.split(' ')
            use_db = dbms.databases.get(vals[1]) 
            dbms.curr_db = use_db
            res = 'Using table: '+ use_db.name
        else:
            res = 'USE a database to implement actions'
    else:
        res = parse(sql_str, dbms.curr_db)
        #res = 'hey'



    #print(sqlglot.parse_one(sql_data))
    return jsonify({'reply':'success', 'result':res})
    #return parsed_sql


if __name__ == "__main__": 
    app.run(debug=True)

