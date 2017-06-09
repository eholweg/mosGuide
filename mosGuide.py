import sqlite3
import re as re
import logging
from logging.config import fileConfig
#import argparse
import os
from datetime import datetime, timedelta


def createTimestamp(d, t):
    dParts = d.split('/')
    if len(dParts[0]) == 1:
        dParts[0] = '0' + dParts[0]
    if len(dParts[1]) == 1:
        dParts[1] = '0' + dParts[1]
    dt_obj = datetime(int(dParts[2]), int(dParts[0]), int(dParts[1]), int(t[:2]), 0, 0)
    return dt_obj

logging.config.fileConfig('logdir/logging.ini')
#Set Logger To 'screen' In Order to View In IDE
#Set Logger To '' In order to view solely in log file
lgr = logging.getLogger('screen')
lgr.info("STARTING APPLICATION")

data = {}
dtgFormat = "%Y-%m-%d %H:%M:%S"
mosDir = 'mosData/0/'

### DB CONNECTION SET UP
lgr.info("MAKING DB CONNECTION")
sqlite_file = "./db/mosdb.db"
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

### GET THE ARGUMENTS FOR MOS FILES TO BE PARSED
#mA = argparse.ArgumentParser()
#mA.add_argument('-mosFiles', nargs='+', type=str, help="Names of MOS files to be parsed", required=True)
#mosArgsFiles = mA.parse_args().mosFiles

for mos in os.listdir(mosDir):
    mosFile = mosDir+mos
    lgr.info("MOS FILE: "+mosFile)

    with open(mosFile) as f:
        for line in f:
            line = line.rstrip().lstrip()

            ### GET SITE
            ### CHECK IF LINE AS MOS GUIDANCE PHRASE
            if re.match('.* MOS GUIDANCE.*', line):
                arr = re.split('\s+', line)
                site = arr[0]
                mod = arr[1]
                tstmp = createTimestamp(arr[4], arr[5])
                newstamp = tstmp.strftime("%Y-%m-%d %H:%M:%S")
                runtime = int(tstmp.strftime("%H"))

                whereVars = (mod, site, runtime)
                lgr.info( "VALS - "+mod+' '+site+' '+str(runtime) )
                c.execute("SELECT modelIndex, timestamp FROM ModelTable WHERE model=? AND site=? and modelRunTime=?",
                          whereVars)
                res = c.fetchone()
                modelIndex = res[0]
                oldTimeStamp = datetime.strptime(res[1], dtgFormat)

                whereVars = (oldTimeStamp.strftime(dtgFormat), mod, site)
                c.execute("UPDATE modelTable SET timestamp=? WHERE model=? AND site=? and modelRunTime=99", whereVars)

                whereVars = (tstmp, mod, site, runtime)
                c.execute("UPDATE modelTable SET timestamp=? WHERE model=? AND site=? and modelRunTime=?", whereVars)

                whereVars = (99,)
                sql = "DELETE FROM minTable WHERE modelIndex=?"
                c.execute(sql, whereVars)
                sql = "DELETE FROM maxTable WHERE modelIndex=?"
                c.execute(sql, whereVars)

                whereVars = (99, modelIndex)
                c.execute("UPDATE maxTable SET modelIndex=? WHERE modelIndex=?", whereVars)
                whereVars = (99, modelIndex)
                c.execute("UPDATE minTable SET modelIndex=? WHERE modelIndex=?", whereVars)
                conn.commit()

            ### STORE REMAINDER OF DATA INTO THE DATA ARRAY
            arr = re.split('\s+', line)
            data[arr.pop(0)] = arr


            # oldTimeStamp += timedelta(hours=24)

    ### NOW HAVE ALL DATA STORED IN THE DATA ARRAY
    ### STORE OFF VALUES INTO DB

    ### MIN/MAX MAX/MIN
    ### IF MODEL RUN IS FROM 00 OR 06 UTC THEN THE FIELD IN 'X/N'
    ### IF MODEL RUN IS FROM 12 OR 18 UTC THEN THE FIELD IS 'N/X'
    ### MAX SHOWS UP AT 00 HOUR... MIN SHOWS UP AT 12 HOUR
    # PRINT DATA['N/X']
    minmax = {}

    ### REMOVE EXISTING ROWS WITH THAT MODEL INDEX
    whereVars = (modelIndex,)
    sql = "DELETE FROM minTable WHERE modelIndex=?"
    c.execute(sql, whereVars)
    sql = "DELETE FROM maxTable WHERE modelIndex=?"
    c.execute(sql, whereVars)
    conn.commit()

    ### BUILD NEW ROWS FROM BULLETIN DATA
    if runtime == 12 or runtime == 18:
        ### STARTS WITH TOMORROW MIN
        minFirst = True
        if runtime == "12":
            tau = 24
        else:
            tau = 18

        for x in data['N/X']:
            minmax[tau] = x
            tau += 12
    else:
        ### STARTS WITH TODAY MAX
        minFirst = False
        if runtime == "0":
            tau = 24
        else:
            tau = 18

        for x in data['X/N']:
            minmax[tau] = x
            tau += 12

    for tau, xn in sorted(minmax.items()):
        # print tau, xn
        whereVars = (modelIndex, tau, int(xn))
        if minFirst:
            sql = "INSERT INTO minTable VALUES (?, ?, ?)"
            c.execute(sql, whereVars)
            conn.commit()
            minFirst = False
        else:
            sql = "INSERT INTO maxTable VALUES (?, ?, ?)"
            c.execute(sql, whereVars)
            conn.commit()
            minFirst = True
            ### MIN/MAX GUIDANCE IS NOW INSERTED FOR THIS SITE MODEL AND TIME


            # print data['KCHA']
            # print data['DT']
            # print data

            # dtg=createTimestamp( data['KCHA'][3], data['KCHA'][4])
            # print dtg.strftime("%A")

            # DAY OF WEEK... MONDAY

            # dtg += timedelta(hours=24)
            # print dtg.strftime(dtgFormat)
            # print dtg.strftime("%A")
            # dtg -= timedelta(hours=24)
            # print dtg.strftime(dtgFormat)
            # print dtg.strftime("%A")

    # Remove the file that has been processed
    os.remove(mosFile)