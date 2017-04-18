import sqlite3
#import pandas as pd
import re as re
from datetime import datetime, timedelta


def createTimestamp( d, t ):
    dParts=d.split('/')
    if len(dParts[0])==1:
        dParts[0]='0'+dParts[0]
    if len(dParts[1])==1:
        dParts[1]='0'+dParts[1]
    dt_obj=datetime( int(dParts[2]), int(dParts[0]), int(dParts[1]), int(t[:2]), 0, 0 )
    return dt_obj


data={}
dtgFormat = "%Y-%m-%d %H:%M:%S"


#DB CONNECTION SET UP
sqlite_file="./db/mosdb.db"
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()


with open('mosData/MAVCHA') as f:
    for line in f:
        line=line.rstrip().lstrip()

        #Get Site
        #Check if line as MOS GUIDANCE PHRASE
        if re.match('.* MOS GUIDANCE.*', line):
            arr = re.split('\s+', line)
            site=arr[0]
            mod=arr[1]
            tstmp=createTimestamp( arr[4], arr[5] )
            newstamp=tstmp.strftime("%Y-%m-%d %H:%M:%S")
            runtime=tstmp.strftime("%H").lstrip('0')

            whereVars=(mod, site, runtime)
            c.execute("SELECT modelIndex, timestamp FROM ModelTable WHERE model=? AND site=? and modelRunTime=?", whereVars)
            res=c.fetchone()
            modelIndex = res[0]
            oldTimeStamp= datetime.strptime(res[1], dtgFormat )

            whereVars=(oldTimeStamp.strftime(dtgFormat), mod, site)
            c.execute("UPDATE modelTable SET timestamp=? WHERE model=? AND site=? and modelRunTime=99", whereVars)

            whereVars=(tstmp, mod, site, runtime)
            c.execute("UPDATE modelTable SET timestamp=? WHERE model=? AND site=? and modelRunTime=?", whereVars)

            #TODO - Need to change the max/min/pop tables in order to reflect modelIndex of 99 for 24 hour old model

            conn.commit()

        #Store remainder of data into the data array
        arr = re.split('\s+', line)
        data[arr.pop(0)] = arr


        #oldTimeStamp += timedelta(hours=24)


# NOW HAVE ALL DATA STORED IN THE DATA ARRAY
# STORE OFF VALUES INTO DB

#MIN/MAX MAX/MIN
# If model run is from 00 or 06 UTC then the field in 'X/N'
# If model run is from 12 or 18 UTC then the field is 'N/X'
# Max shows up at 00 hour... Min shows up at 12 hour
#print data['N/X']
minmax={}

# REMOVE EXISTING ROWS WITH THAT MODEL INDEX
whereVars=( modelIndex, )
sql = "DELETE FROM minTable WHERE modelIndex=?"
c.execute(sql, whereVars)
sql = "DELETE FROM maxTable WHERE modelIndex=?"
c.execute(sql, whereVars)
conn.commit()

# BUILD NEW ROWS FROM BULLETIN DATA
if runtime=="12" or runtime=="18":
    # STARTS WITH TOMORROW MIN
    minFirst=True
    if runtime=="12":
        tau=24
    else:
        tau=18

    for x in data['N/X']:
        minmax[tau] = x
        tau += 12
else:
    # STARTS WITH TODAY MAX
    minFirst=False
    if runtime=="0":
        tau=24
    else:
        tau=18

    for x in data['X/N']:
        minmax[tau] = x
        tau += 12

for tau, xn in sorted( minmax.items() ):
    #print tau, xn
    whereVars=( modelIndex, tau, int(xn) )
    if minFirst:
        sql = "INSERT INTO minTable VALUES (?, ?, ?)"
        c.execute(sql, whereVars)
        conn.commit()
        minFirst=False
    else:
        sql = "INSERT INTO maxTable VALUES (?, ?, ?)"
        c.execute(sql, whereVars)
        conn.commit()
        minFirst = True
# - MIN/MAX GUIDANCE IS NOW INSERTED FOR THIS SITE MODEL AND TIME


#print data['KCHA']
#print data['DT']
#print data

#dtg=createTimestamp( data['KCHA'][3], data['KCHA'][4])
#print dtg.strftime("%A")

#DAY OF WEEK... MONDAY

#dtg += timedelta(hours=24)
#print dtg.strftime(dtgFormat)
#print dtg.strftime("%A")
#dtg -= timedelta(hours=24)
#print dtg.strftime(dtgFormat)
#print dtg.strftime("%A")

