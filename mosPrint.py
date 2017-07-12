import sqlite3
import logging
from logging.config import fileConfig
import argparse
import os
import pytz
from datetime import datetime, timedelta

def createTimestamp(d, t):
    dParts = d.split('/')
    if len(dParts[0]) == 1:
        dParts[0] = '0' + dParts[0]
    if len(dParts[1]) == 1:
        dParts[1] = '0' + dParts[1]
    dt_obj = datetime(int(dParts[2]), int(dParts[0]), int(dParts[1]), int(t[:2]), 0, 0)
    return dt_obj

def getDeltaModel(stamp, hours):
    rt = datetime.strptime(stamp, dtgFormat)
    newDtg=rt-timedelta(hours=hours)
    return newDtg.strftime(dtgFormat)

def makeDictionary(data):
    dict = {}
    for t in data:
        dict[t[0]] = t[1]
    return dict

def createHeader(runtime):
    rt = datetime.strptime(runtime, dtgFormat)
    header="Model Run Time "
    header+=rt.strftime('%m/%d/%Y %H%M')+"\n"
    header+='{0:<6s}{1}{2:>4s}{3:>4s}{4:>6s}{5:^10}{6}{7:>4s}{8:>4s}{9:>6s}\n'.format(
                            '***',
                            'Temps',
                            'MET',
                            'MAV',
                            'Blend',
                            '******',
                            'POPS',
                            'MET',
                            'MAV',
                            'Blend'
                )
    return header

def createDateLabels(runtime, tau):
    typeData="Min"
    nightVal=""
    newTime = runtime + timedelta(hours=tau)
    newTimeUTC = newTime.replace(tzinfo=pytz.timezone('UTC'))

    if tau<25:
        if newTime.hour==0:
            dayVal="Today"
            typeData="Max"
        else:
            dayVal="Tonight"
    else:
        etz=pytz.timezone('US/Eastern')
        easternTime=newTimeUTC.astimezone(etz)
        if newTimeUTC.hour == 0:
            nightVal = ""
            dayVal = easternTime.strftime("%A")  # Sunday
            typeData="Max"
        else:
            easternTime = easternTime - timedelta(hours=12)
            dayVal = easternTime.strftime("%A")  # Sunday
            nightVal = " Night"

    return '{0:<25s}{1:}\n'.format( typeData, dayVal+nightVal )

def createBody(sites, dtg, maxNam, minNam, popNam, maxGfs, minGfs, popGfs):
    dataVals=''
    dValsNam = {}
    dValsGfs = {}
    rt = datetime.strptime(dtg, dtgFormat)
    rtHr = rt.hour

    #Get all sites in DB
    for s in sites:
        dValsNam[s] = maxNam[s].copy()
        dValsNam[s].update(minNam[s])
        dValsGfs[s] = maxGfs[s].copy()
        dValsGfs[s].update(minGfs[s])

    # IF 0 OR 6 RUN... MAX WILL BE FIRST
    # ... 6Z starts at 18 hours
    # ... 0Z starts at 24 hours
    # IF 12 OR 18 RUN... MIN WILL BE FIRST
    # ... 18Z starts at 18 hours
    # ... 12Z starts at 24 hours
    if rtHr < 7:
        if rtHr == 0:
            tauSt = 24
        else:
            tauSt = 18
        lgr.info("RUN HOUR: " + str(rtHr))
        lgr.info("MAX VALUES FIRST")
        lgr.info("TAU START AT: " + str(tauSt))

        for t in xrange(tauSt, 84, 12):
            # Use time delta and runtime...
            # Calculate Day of Week
            dayOfWeek = createDateLabels(rt, t)
            dataVals+=dayOfWeek
    else:
        if rtHr == 12:
            tauSt = 24
        else:
            tauSt = 18
        lgr.info("RUN HOUR: " + str(rtHr))
        lgr.info("MIN VALUES FIRST")
        lgr.info("TAU START AT: " + str(tauSt))

    for t in xrange(tauSt, 84, 12):
        dataVals += createDateLabels(rt, t)
        for s in sites:
            btemp=int( round( (dValsNam[s][t] + dValsGfs[s][t]) / 2, 0 ) )
            bpop=int( round( (popNam[s][t] + popGfs[s][t]) / 2, 0 ) )

            dataVals += '{0:<10s}{1:>5d}{2:>4d}{3:>6d}{4:<14s}{5:>4d}{6:>4d}{7:>6d}\n'.format(
                s[1:],
                dValsNam[s][t],
                dValsGfs[s][t],
                btemp,
                ' ',
                popNam[s][t],
                popGfs[s][t],
                bpop
            )
        dataVals += "\n"
    return dataVals

def createFooter():
    footer="""SHORT-TERM
        AFD___ ZFP(GEN)___ SYN___ FWF___ HWO(GEN)___ NFDRS___
  
LONG-TERM
        WRKAFD___ ZFP(XMIT)___  HWO(XMIT)___
 
GRIDS
    T___ DP___ MAX___ MIN___ WND___ SKY___ WX___POP___QPF___SNW___ICE___
 
    Haines___ MixHgt___ TransWind___ Forecaster Number___ 
 
VERIFY RUN/SENT
              FWM___ SFT___ AFM___ PFM___
  """
    return footer



#.....................................................................#
#.....................................................................#
# MAIN PROGRAM FOR PRINTING OUT MOS DATA                              #
#.....................................................................#
#.....................................................................#

# Get the path to the executable python
curFile = __file__
realPath = os.path.realpath(curFile)
dirPath = os.path.dirname(realPath)

logging.logFileName = dirPath + '/logdir/FTPprint.log'
logging.config.fileConfig(dirPath + "/logdir/logging.ini")
# Set Logger To 'screen' In Order to View In IDE
# Set Logger To '' In order to view solely in log file
lgr = logging.getLogger('screen')
lgr.info("STARTING MOS GUIDE PRINTER APPLICATION")

data = {}
dtgFormat = "%Y-%m-%d %H:%M:%S"
mosDir = dirPath + '/mosData/'

### DB CONNECTION SET UP
lgr.info("MAKING MOSGUIDE DB CONNECTION")
sqlite_file = dirPath + "/db/mosdb.db"
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

### GET THE ARGUMENTS FOR MOS FILES TO BE PARSED
mA = argparse.ArgumentParser()
mA.add_argument('-mosFiles', nargs='+', type=str, help="Names of MOS files to be printed", required=True)
mosArgsFiles = mA.parse_args().mosFiles

vtimesnam = {}
trendnam = {}
vtimesgfs = {}
trendgfs = {}
maxValsArrNam = {}
maxTrendValsArrNam = {}
minValsArrNam = {}
minTrendValsArrNam = {}
popValsArrNam = {}
popTrendValsArrNam = {}
maxValsArrGfs = {}
maxTrendValsArrGfs = {}
minValsArrGfs = {}
minTrendValsArrGfs = {}
popValsArrGfs = {}
popTrendValsArrGfs = {}

if mosArgsFiles[0] == "ALL":
    lgr.info("GOING TO RETRIEVE ALL SITES")
    c.execute("SELECT siteid FROM mosGuideAdmin ORDER BY displayOrder")
    sitesData = c.fetchall()
    sites = []
    for s in sitesData:
        sites.append(s[0])

    for sa in sites:
        site = sa
        lgr.info("SITE: " + site)

        q = "SELECT timestamp FROM modelTable WHERE model=? AND site=? ORDER BY timestamp DESC LIMIT 1"
        whereVars = ('NAM', site)
        c.execute(q, whereVars)
        latestNam = c.fetchone()[0]
        lgr.info("LATEST NAM: " + latestNam)
        vtimesnam[site] = latestNam
        trendnam[site] = getDeltaModel(latestNam, 12)

        whereVars = ('GFS', site)
        c.execute(q, whereVars)
        latestGfs = c.fetchone()[0]
        lgr.info("LATEST GFS: " + latestGfs)
        vtimesgfs[site] = latestGfs
        trendgfs[site] = getDeltaModel(latestGfs, 12)

    # NAM MOS
    #print vtimesnam
    #print trendnam
    haveNamTrend=False
    if len(set(vtimesnam.values())) == 1:
        if len(set(trendnam.values())) == 1:
            lgr.info("NAM HAS TREND TIMES THAT ARE EQUAL - Will include trend data")
            haveNamTrend = True
            trendTime= trendnam.values()[0]

        lgr.info("NAM TIMES EQUAL")
        q = "SELECT modelIndex from modelTable WHERE model=? AND timestamp = ? and site=?"
        qMax = "SELECT tau, maxVal from maxTable WHERE modelIndex=? ORDER BY tau"
        qMin = "SELECT tau, minVal from minTable WHERE modelIndex=? ORDER BY tau"
        qPop = "SELECT tau, popVal from popPdTable WHERE modelIndex=? ORDER BY tau"

        for sa in sites:
            site = sa
            whereVars = ('NAM', latestNam, site)
            c.execute(q, whereVars)
            modelIndex = c.fetchone()[0]
            whereVars = (modelIndex,)
            c.execute(qMax, whereVars)
            maxValsArrNam[site] = makeDictionary(c.fetchall())
            c.execute(qMin, whereVars)
            minValsArrNam[site] = makeDictionary(c.fetchall())
            c.execute(qPop, whereVars)
            popValsArrNam[site] = makeDictionary(c.fetchall())

            #NOW GET 12 HOUR OLD GUIDANCE
            if haveNamTrend:
                whereVars = ('NAM', trendTime, site)
                c.execute(q, whereVars)
                modelIndex = c.fetchone()[0]
                whereVars = (modelIndex,)
                c.execute(qMax, whereVars)
                maxTrendValsArrNam[site] = makeDictionary(c.fetchall())
                c.execute(qMin, whereVars)
                minTrendValsArrNam[site] = makeDictionary(c.fetchall())
                c.execute(qPop, whereVars)
                popTrendValsArrNam[site] = makeDictionary(c.fetchall())


        # HAVE ALL NAM VALUES... NOW NEED TO OUTPUT
        lgr.info("SENDING DATA TO CREATE PRINT")
    else:
        lgr.info("NAM TIMES DIFFER")
        lgr.info("Cannot create output for latest run time.")
        lgr.info("Exiting")
        exit(9)

    # GFS MOS
    haveGfsTrend = False
    if len(set(vtimesgfs.values())) == 1:
        if len(set(trendgfs.values())) == 1:
            lgr.info("GFS HAS TREND TIMES THAT ARE EQUAL - Will include trend data")
            haveGfsTrend = True
            trendTime= trendgfs.values()[0]

        lgr.info("GFS TIMES EQUAL")
        q = "SELECT modelIndex from modelTable WHERE model=? AND timestamp = ? and site=?"
        qMax = "SELECT tau, maxVal from maxTable WHERE modelIndex=? ORDER BY tau"
        qMin = "SELECT tau, minVal from minTable WHERE modelIndex=? ORDER BY tau"
        qPop = "SELECT tau, popVal from popPdTable WHERE modelIndex=? ORDER BY tau"

        for sa in sites:
            site = sa
            whereVars = ('GFS', latestGfs, site)
            c.execute(q, whereVars)
            modelIndex = c.fetchone()[0]
            whereVars = (modelIndex,)
            c.execute(qMax, whereVars)
            maxValsArrGfs[site] = makeDictionary(c.fetchall())
            c.execute(qMin, whereVars)
            minValsArrGfs[site] = makeDictionary(c.fetchall())
            c.execute(qPop, whereVars)
            popValsArrGfs[site] = makeDictionary(c.fetchall())

            # NOW GET 12 HOUR OLD GUIDANCE
            if haveGfsTrend:
                whereVars = ('GFS', trendTime, site)
                c.execute(q, whereVars)
                modelIndex = c.fetchone()[0]
                whereVars = (modelIndex,)
                c.execute(qMax, whereVars)
                maxTrendValsArrGfs[site] = makeDictionary(c.fetchall())
                c.execute(qMin, whereVars)
                minTrendValsArrGfs[site] = makeDictionary(c.fetchall())
                c.execute(qPop, whereVars)
                popTrendValsArrGfs[site] = makeDictionary(c.fetchall())
    else:
        lgr.info("GFS TIMES DIFFER")
        exit(9)





    output = createHeader(latestNam)
    output += createBody(sites, latestNam,
                         maxValsArrNam, minValsArrNam, popValsArrNam,
                         maxValsArrGfs, minValsArrGfs, popValsArrGfs,
                         )
    output += createFooter()
    print output.upper()

else:
    lgr.info("GOING TO RETRIEVE THE FOLLOWING SITES: ")
    for site in mosArgsFiles:
        lgr.info("SITE: " + site)

exit(0)