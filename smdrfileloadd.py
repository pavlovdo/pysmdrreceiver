#!/usr/bin/env python

import sys, time, ConfigParser
from daemon import Daemon
from socket import *                                    # get socket constructor and constants

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
        except:
            print "Check the configuration file " + orbconffile
            listenif = ''
            listenport = 9910
            recvbuff = 1
            logfile = '/var/log/avaya/smdr.log'

        smdrrecord = []
        smdrcolumn = ''

        smdrlog = open(logfile, 'a')

        listensock = socket(AF_INET, SOCK_STREAM)                  # make a TCP socket object
        listensock.bind((listenif, listenport))                    # bind it to server port number
        listensock.listen(1)                                       # listen, allow 1 pending connects

        while True:                                             # listen until process killed
            avcon, address = listensock.accept()          # wait for next client connect
            while True:
                smdrdata = avcon.recv(recvbuff)
                if not smdrdata: 
                    break
                if smdrdata in (',','\r'):
                    smdrrecord.append(smdrcolumn)
                    smdrcolumn = ''
                elif smdrdata == '\n':
                    smdrrecord.pop(19)
                    smdrrecord = []
                else:
                    smdrcolumn += smdrdata
                smdrlog.write(smdrdata)
                smdrlog.flush()
        avcon.close()
        smdrlog.close()

if __name__ == "__main__":
	daemon = MyDaemon('/var/run/smdrfileloadd.pid')
#        daemon = Daemon('/var/run/smdrrec.pid')
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
