#!/usr/bin/env python

import sys
import os
import ConfigParser
from daemon import Daemon
from socket import socket, AF_INET, SOCK_STREAM
from datetime import datetime


class MyDaemon(Daemon):

    def run(self):

        orbconffile = '/etc/orbit/orbit.cfg'
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

#       make a TCP socket object
        listensock = socket(AF_INET, SOCK_STREAM)
#       bind it to server port number
        listensock.bind((listenif, listenport))
#       listen, allow 1 pending connects
        listensock.listen(1)

#       listen until process killed
        while True:
            # wait for next client connect
            avcon, address = listensock.accept()
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
                    logtempfile = logfile + '.' + curweek + '.'
                    + curday + '.' + curhour
                    smdrlog = open(logdir + logtempfile, 'a')
                smdrlog.write(smdrdata)
                smdrlog.flush()
        avcon.close()
        smdrlog.close()

if __name__ == "__main__":
    daemon = MyDaemon('/var/run/smdrfileloadd.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
            sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
