#!/usr/bin/env python

from socket import *                                    # get socket constructor and constants
import MySQLdb, ConfigParser

orbconffile = '/etc/orbit/orbit.cfg'

# Read the Orbit configuration file
config = ConfigParser.RawConfigParser()
try:
    config.read(orbconffile)
    dbhost = config.get('OrbitDatabase', 'dbhost')
    dbname = config.get('OrbitDatabase', 'dbname')
    table = config.get('OrbitDatabase', 'avsmdrtable')
    listenif = config.get('SMDRConfig', 'listenif')
    listenport = config.getint('SMDRConfig', 'listenport')
    recvbuff = config.getint('SMDRConfig', 'recvbuff')
    logfile = config.get('SMDRConfig', 'logfile')
except:
    print "Check the configuration file " + orbconffile
    dbhost = 'localhost'
    dbname = 'orbit'
    table = 'avaya_smdrs'
    listenif = ''
    listenport = 9910
    recvbuff = 1
    logfile = '/var/log/avaya/smdr.log'

smdrrecord = []
smdrcolumn = ''

smdrlog = open(logfile, 'a')
smdrsqlins = ("INSERT INTO " + dbname + '.' + table +
               "(callstart, connectedtime, ringtime, caller, direction, callednumber, dialednumber, account, isinternal, callid, continuation, party1device, party1name,"
               "party2device, party2name, holdtime, parktime, authvalid, authcode, callcharge, currency, amount_at_last_user_change, callunits, units_at_last_user_change,"
               "costperunit, markup, external_targeting_cause, external_targeter_id, external_targeted_number) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

# Connect to Orbit database
try:
    dbconn = MySQLdb.connect(host=dbhost, db=dbname, read_default_file="/etc/orbit/.my.cnf", charset='utf8') 
except MySQLdb.OperationalError as e:
    print "Can't connect to Orbit database. Check connection to MySQL server"
    print e

dbcursor = dbconn.cursor() 

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
            print smdrrecord
            dbcursor.execute(smdrsqlins, smdrrecord)
            dbconn.commit()
            smdrrecord = []
        else:
            smdrcolumn += smdrdata
        smdrlog.write(smdrdata)
        smdrlog.flush()
avcon.close()
smdrlog.close()
dbcursor.close()
dbconn.close()
"""
            try:
                dbcursor.execute(smdrsqlins, smdrrecord)
                dbconn.commit()
                smdrrecord = []
            except OperationalError as e:
                if 'MySQL server has gone away' in str(e):
                    #do what you want to do on the error
                    for i in list(range(1, 11)):
                        dbconn = MySQLdb.connect(host=dbhost, db=dbname, read_default_file="/etc/orbit/.my.cnf", charset='utf8')
                            if dbconn:
                                dbcursor = dbconn.cursor()
                                print e
                                break
                            else:
                                raise e()
                else:
                    raise e()
"""
