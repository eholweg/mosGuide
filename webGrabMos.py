import urllib2
from bs4 import BeautifulSoup
import os

sites = ( 'KCHA',
          'KRHP',
          'KTYS',
          'KOQT',
          'KTRI',
          'KVJI',
          'KLNP'
          )

for s in sites:
    link='http://www.nws.noaa.gov/cgi-bin/mos/getmav.pl?sta='+s
    content = urllib2.urlopen(link).read()
    soup = BeautifulSoup(content, "html.parser")
    pre = soup.find('pre')
    mav = pre.text
    mavFile='mosData/MAV'+s[1:]
    f=open(mavFile, 'w' )
    f.write(mav)
    f.close()
    print mav
    os.system("mosGuide.py -mosFiles MAV"+ s[1:] )


    link = 'http://www.nws.noaa.gov/cgi-bin/mos/getmet.pl?sta=' + s
    content = urllib2.urlopen(link).read()
    soup = BeautifulSoup(content, "html.parser")
    pre = soup.find('pre')
    met = pre.text
    metFile='mosData/MET' + s[1:]
    f = open(metFile, 'w' )
    f.write(met)
    f.close()
    print met
    os.system("mosGuide.py -mosFiles MET" + s[1:] )