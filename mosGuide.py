import sqlite3
import pandas as pd
import re as re
import datetime

sqlite_file="./db/mosdb.db"

#conn = sqlite3.connect(sqlite_file, detect_types=sqlite3.PARSE_DECLTYPES)

#conn.execute("CREATE TABLE models (id integer primary key, modelrun int, model text, site text, [timestamp] timestamp)")

#conn.execute("CREATE TABLE max (mdlid integer, max int, tau int)")

data={}
with open('mosData/MAVCHA') as f:
    for line in f:
        arr=re.split('\s+', line.rstrip().lstrip())
        data[arr.pop(0)]=arr
        #print arr
        #data=pd.read_csv(line, delimiter=' ')
        #data

print data['N/X']
print data['KCHA']
print data['DT']
print data