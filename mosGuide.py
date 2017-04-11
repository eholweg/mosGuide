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
    dt_obj=datetime( int(dParts[2]), int(dParts[1]), int(dParts[0]), int(t[:2]), 0, 0 )
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
        #print line

        #Get Site
        #Check if line as MOS GUIDANCE PHRASE
        if re.match('.* MOS GUIDANCE.*', line):
            arr = re.split('\s+', line)
            site=arr[0]
            mod=arr[1]
            tstmp=createTimestamp( arr[4], arr[5] )
            newstamp=tstmp.strftime("%Y-%m-%d %H:%M:%S")
            runtime=tstmp.strftime("%H").lstrip('0')

            sql = "SELECT modelIndex, timestamp FROM ModelTable WHERE model='"+mod+"' AND site='"+site+"'and modelRunTime="+runtime+"\n"
            whereVars=(mod, site, runtime)
            c.execute("SELECT modelIndex, timestamp FROM ModelTable WHERE model=? AND site=? and modelRunTime=?", whereVars)
            res=c.fetchone()
            modelIndex = res[0]
            oldTimeStamp= datetime.strptime(res[1], dtgFormat )
            oldTimeStamp += timedelta(hours=24)

            whereVars=(oldTimeStamp.strftime(dtgFormat), mod, site)
            c.execute("UPDATE modelTable SET timestamp=? WHERE model=? AND site=? and modelRunTime=99", whereVars)
            conn.commit()


            print c.rowcount
            print whereVars



            #sql = "SELECT modelIndex FROM ModelTable WHERE model='"+mod+"' AND site='"+site+"'and modelRunTime='99'\n"
            res2='##24HrIndex##'
            #sql += "UPDATE ModelTable SET timestamp='"+newstamp+"' WHERE model='"+mod+"' AND site='"+site+"' and modelRunTime="+runtime+"\n"
            # NOW CLEAN OUT MIN/MAX/POP TABLES TO RECIEVE NEW DATA
            #sql += "UPDATE minTable SET modelIndex='"+res2+"' WHERE modelIndex='"+res+"'\n"

            print sql

        #If so first four characters are site ID,
        #Next three chars are model



        #Get Hours

        #Get MaxMin

        #Get Temp



        arr=re.split('\s+', line)
        data[arr.pop(0)]=arr
        #print arr
        #data=pd.read_csv(line, delimiter=' ')
        #data


# If model run is from 00 or 06 UTC then the field in 'X/N'
# otherwise the field is 'N/X'


print data['N/X']
print data['KCHA']
print data['DT']
print data

dtg=createTimestamp( data['KCHA'][3], data['KCHA'][4])
print dtg.strftime("%A")

dtg += timedelta(hours=24)

print dtg.strftime(dtgFormat)
print dtg.strftime("%A")

dtg -= timedelta(hours=24)

print dtg.strftime(dtgFormat)
print dtg.strftime("%A")

