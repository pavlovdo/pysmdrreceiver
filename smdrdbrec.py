#!/usr/bin/env python

from socket import *                                    # get socket constructor and constants
import MySQLdb

smdrlistenint = ''                              # '' = all available interfaces on host
smdrlistenport = 9910                                      # listen on a non-reserved port number
smdrdbhost = 'localhost'
smdrrecvbuff = 1
smdrrecord = []
smdrcolumn = ''
smdrdb = 'orbit'
smdrtable = 'avaya_smdr'
smdrlog = '/var/log/avaya/smdr.log'

smdrlog = open(smdrlog, 'a')
smdrsqlins = ("INSERT INTO " + smdrdb + '.' + smdrtable +
               "(callstart, connectedtime, ringtime, caller, direction, callednumber, dialednumber, account, isinternal, callid, continuation, party1device, party1name,"
               "party2device, party2name, holdtime, parktime, authvalid, authcode, callcharge, currency, amount_at_last_user_change, callunits, units_at_last_user_change,"
               "costperunit, markup, external_targeting_cause, external_targeter_id, external_targeted_number) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

smdrdbconn = MySQLdb.connect(host=smdrdbhost, db=smdrdb, read_default_file="~/.my.cnf", charset='utf8')
smdrdbcursor = smdrdbconn.cursor() 

smdrlistensock = socket(AF_INET, SOCK_STREAM)                  # make a TCP socket object
smdrlistensock.bind((smdrlistenint, smdrlistenport))                    # bind it to server port number
smdrlistensock.listen(5)                                       # listen, allow 5 pending connects

while True:                                             # listen until process killed
    avayaconnect, address = smdrlistensock.accept()          # wait for next client connect
    while True:
        smdrdata = avayaconnect.recv(smdrrecvbuff)
        if not smdrdata: break
        if smdrdata in (',','\r'):
                smdrrecord.append(smdrcolumn)
                smdrcolumn = ''
        elif smdrdata == '\n':
                smdrrecord.pop(19)
                print smdrrecord
                smdrdbcursor.execute(smdrsqlins, smdrrecord)
                smdrdbconn.commit()
                smdrrecord = []
        else:
                smdrcolumn += smdrdata
        smdrlog.write(smdrdata)
        smdrlog.flush()
    avayaconnect.close()
    smdrlog.close()
    smdrdbcursor.close()
