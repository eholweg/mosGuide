import urllib2
from bs4 import BeautifulSoup
import os
import subprocess

sites = ( 'KCHA',
          'KRHP',
          'KTYS',
          'KOQT',
          'KTRI',
          'KVJI',
          'KLNP'
          )

curFile=__file__
realPath = os.path.realpath(curFile)
dirPath = os.path.dirname(realPath)

pyCmd = "U:\python\python.exe "

for s in sites:
    link='http://www.nws.noaa.gov/cgi-bin/mos/getmav.pl?sta='+s
    content = urllib2.urlopen(link).read()
    soup = BeautifulSoup(content, "html.parser")
    pre = soup.find('pre')
    mav = pre.text
    mavFile=dirPath+'/mosData/MAV'+s[1:]
    f=open(mavFile, 'w' )
    f.write(mav)
    f.close()
    cmd = pyCmd + dirPath + '/mosGuide.py -mosFiles MAV' + s[1:]
    print mav
    print cmd
    process = subprocess.Popen(cmd)
    print process.communicate()[0]

    link = 'http://www.nws.noaa.gov/cgi-bin/mos/getmet.pl?sta=' + s
    content = urllib2.urlopen(link).read()
    soup = BeautifulSoup(content, "html.parser")
    pre = soup.find('pre')
    met = pre.text
    metFile=dirPath+'/mosData/MET' + s[1:]
    f = open(metFile, 'w' )
    f.write(met)
    f.close()
    cmd = pyCmd + dirPath + '/mosGuide.py -mosFiles MET' + s[1:]
    print met
    print cmd
    process = subprocess.Popen(cmd)
    print process.communicate()[0]