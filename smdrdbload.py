#!/usr/bin/env python

#from socket import *                                    # get socket constructor and constants
import MySQLdb
import ConfigParser
import sys

if len(sys.argv) == 2:
    logfile = sys.argv[1]
else:
    print "Error in command line. Usage: %s logfile_name" % sys.argv[0]
    sys.exit(2)

orbconffile = '/etc/orbit/orbit.cfg'

# Read the Orbit configuration file
config = ConfigParser.RawConfigParser()
try:
    config.read(orbconffile)
    dbhost = config.get('OrbitDatabase', 'dbhost')
    dbname = config.get('OrbitDatabase', 'dbname')
    table = config.get('OrbitDatabase', 'avsmdrtable')
    archlogdir = config.get('SMDRConfig', 'archlogdir')
except:
    print "Check the configuration file " + orbconffile
    dbhost = 'localhost'
    dbname = 'orbit'
    table = 'avaya_smdrs'
    archlogdir = '/var/log/avaya_arch_logs/'

if len(logfile.split('/')) <= 1:
    logfile = archlogdir + logfile

try:
    avsmdrlog = open(logfile, 'r')
except:
    print "Log file %s does not exist" % logfile
    sys.exit(0)
  
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

for line in avsmdrlog:
    smdrrecord = line.split(',')
#    print len (smdrrecord)  
    if len (smdrrecord) != 30:
        print "Bad SMDR in file %s" % logfile
    else:
        smdrrecord.pop(19)
        print smdrrecord
        try:
            dbcursor.execute(smdrsqlins, smdrrecord)
            dbconn.commit()
        except:
            print "Can't load data to database %s" % dbname

avsmdrlog.close()
dbcursor.close()
dbconn.close()
