import sqlite3
import logging
from logging.config import fileConfig
import argparse
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


def createPrint(sites, dtg, max, min, pop):

    dVals={}

    rt = datetime.strptime(dtg, dtgFormat)
    rtHr= rt.hour

    print max
    print max['KTYS'][0]

    return

    for s in sites:
        #print s
        print max['KCHA']
        print type(max['KCHA'])

        dVals[s]=max[s].copy()
        dVals[s].update(min[s])

    #IF 0 OR 6 RUN... MAX WILL BE FIRST
    #... 6Z starts at 18 hours
    #... 0Z starts at 24 hours
    # IF 12 OR 18 RUN... MIN WILL BE FIRST
    #... 18Z starts at 18 hours
    #... 12Z starts at 24 hours
    if rtHr==0 or rtHr==6:
        if rtHr==0:
            tauSt=24
        else:
            tauSt=18
        lgr.info("RUN HOUR: " + str(rtHr))
        lgr.info("MAX VALUES FIRST")
        lgr.info("TAU START AT: " + str(tauSt))

        for t in xrange(tauSt, 72, 12):
            for s in sites:
                print('{} {} - {}'.format(str(rt + timedelta(hours=t)),s, dVals[s][t]))


        #print max
        #for s in maxValsArr:
        #    print s
        #    print maxValsArr[s]
        #print str(rt + timedelta(hours=18))
        #print str(rt + timedelta(hours=30))



    else:
        if rtHr==12:
            tauSt=24
        else:
            tauSt=18
        lgr.info("RUN HOUR: " + str(rtHr))
        lgr.info("MIN VALUES FIRST")
        lgr.info("TAU START AT: " + str(tauSt))

        for t in xrange(tauSt, 72, 12):
            for s in sites:
                print('{} {} - {}'.format(str(rt + timedelta(hours=t)), s, dVals[s][t]))

        #print min
        #for s in minValsArr:
        #    print s
        #    print minValsArr[s]


        #print(rt)
        #print("Add 18 then 30 hrs")
        #print str( rt + timedelta(hours=24) )
        #print str( rt + timedelta(hours=30) )


    return








#Get the path to the executable python
curFile=__file__
realPath = os.path.realpath(curFile)
dirPath = os.path.dirname(realPath)

logging.logFileName = dirPath+'/logdir/FTPprint.log'
logging.config.fileConfig(dirPath + "/logdir/logging.ini")
#Set Logger To 'screen' In Order to View In IDE
#Set Logger To '' In order to view solely in log file
lgr = logging.getLogger('screen')
lgr.info("STARTING MOS GUIDE PRINTER APPLICATION")

data = {}
dtgFormat = "%Y-%m-%d %H:%M:%S"
mosDir = dirPath+'/mosData/'

### DB CONNECTION SET UP
lgr.info("MAKING MOSGUIDE DB CONNECTION")
sqlite_file = dirPath+"/db/mosdb.db"
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

### GET THE ARGUMENTS FOR MOS FILES TO BE PARSED
mA = argparse.ArgumentParser()
mA.add_argument('-mosFiles', nargs='+', type=str, help="Names of MOS files to be printed", required=True)
mosArgsFiles = mA.parse_args().mosFiles

vtimesnam={}
vtimesgfs={}
maxValsArr={}
minValsArr={}
popValsArr={}

if mosArgsFiles[0]=="ALL":
    lgr.info("GOING TO RETRIEVE ALL SITES")
    c.execute("SELECT DISTINCT site FROM ModelTable")
    sites=c.fetchall()
    for sa in sites:
        site=sa[0]
        lgr.info("SITE: "+ site)

        q = "SELECT timestamp FROM modelTable WHERE model=? AND site=? ORDER BY timestamp DESC LIMIT 1"
        whereVars = ('NAM', site)
        c.execute(q, whereVars)
        latestNam=c.fetchone()[0]
        lgr.info("LATEST NAM: "+latestNam)
        vtimesnam[site]=latestNam

        whereVars = ('GFS', site)
        c.execute(q, whereVars)
        latestGfs=c.fetchone()[0]
        lgr.info("LATEST GFS: "+latestGfs)
        vtimesgfs[site]=latestGfs

    # NAM MOS
    if len(set(vtimesnam.values()))==1:
        lgr.info("NAM TIMES EQUAL")
        q = "SELECT modelIndex from modelTable WHERE model=? AND timestamp = ? and site=?"
        qMax = "SELECT tau, maxVal from maxTable WHERE modelIndex=? ORDER BY tau"
        qMin = "SELECT tau, minVal from minTable WHERE modelIndex=? ORDER BY tau"
        qPop = "SELECT tau, popVal from popPdTable WHERE modelIndex=? ORDER BY tau"

        for sa in sites:
            site = sa[0]
            whereVars=('NAM', latestNam, site)
            c.execute(q, whereVars)
            modelIndex=c.fetchone()[0]
            whereVars=(modelIndex,)
            c.execute(qMax, whereVars)
            maxValsArr[site]=c.fetchall()
            c.execute(qMin, whereVars)
            minValsArr[site]=c.fetchall()
            c.execute(qPop, whereVars)
            popValsArr[site]=c.fetchall()

        # HAVE ALL NAM VALUES... NOW NEED TO OUTPUT
        lgr.info("SENDING DATA TO CREATE PRINT")
        createPrint(sites, latestNam, maxValsArr, minValsArr, popValsArr)


    else:
        lgr.info("NAM TIMES DIFFER")

    #GFS MOS
    if len(set(vtimesgfs.values()))==1:
        lgr.info("GFS TIMES EQUAL")
    else:
        lgr.info("GFS TIMES DIFFER")


    # whereVars = (mod, site, runtime)
    # lgr.info("MODEL DETAILS - " + mod + ' ' + site + ' ' + str(runtime))
    # c.execute(")
    # res = c.fetchone()
    # modelIndex = res[0]
    # oldTimeStamp = datetime.strptime(res[1], dtgFormat)

else:
    lgr.info("GOING TO RETRIEVE THE FOLLOWING SITES: ")
    for site in mosArgsFiles:
        lgr.info("SITE: "+site)


exit(0)