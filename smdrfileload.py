#!/usr/bin/env python

from socket import *                                    # get socket constructor and constants
import ConfigParser, os
from datetime import datetime

orbconffile = '/etc/orbit/orbit.cfg'

# Read the Orbit configuration file
config = ConfigParser.RawConfigParser()
try:
    config.read(orbconffile)
    listenif = config.get('SMDRConfig', 'listenif')
    listenport = config.getint('SMDRConfig', 'listenport')
    recvbuff = config.getint('SMDRConfig', 'recvbuff')
    logfile = config.get('SMDRConfig', 'logfile')
    logdir = config.get('SMDRConfig', 'logdir')
    archlogdir = config.get('SMDRConfig', 'archlogdir')
except:
    print "Check the configuration file " + orbconffile
    listenif = ''
    listenport = 9910
    recvbuff = 1
    logfile = 'smdr.log'
    logdir = '/var/log/avaya/'
    archlogdir = '/var/log/avaya_arch_logs/'

curhour = datetime.now().strftime('%H')
curday = datetime.now().strftime('%w')
curweek = datetime.now().strftime('%W')
logtempfile = logfile + '.' + curweek + '.' + curday + '.' + curhour
smdrlog = open(logdir + logtempfile, 'a')

listensock = socket(AF_INET, SOCK_STREAM)                  # make a TCP socket object
listensock.bind((listenif, listenport))                    # bind it to server port number
listensock.listen(1)                                       # listen, allow 1 pending connects

while True:                                             # listen until process killed
    avcon, address = listensock.accept()          # wait for next client connect
    while True:
        smdrdata = avcon.recv(recvbuff)
        if not smdrdata: 
            break
        if curhour != datetime.now().strftime('%H'):
            smdrlog.close()
            os.rename(logdir + logtempfile, archlogdir + logtempfile)
            curweek = datetime.now().strftime('%W')
            curhour = datetime.now().strftime('%H')
            curday = datetime.now().strftime('%w') 
            logtempfile = logfile + '.' + curweek + '.' + curday + '.' + curhour
            smdrlog = open(logdir + logtempfile, 'a') 
        smdrlog.write(smdrdata)
        smdrlog.flush()
avcon.close()
smdrlog.close()
