'''
Created on Mar 24, 2013

@author: amitabul
'''
import imp
import sys
import datetime
from asap.exporter import Exporter
from asap.pusher import Pusher
from asap.sql_reader import SqlReader

class Transmitter():
    
    def __init__(self, conf):
        self.conf = conf
        self.conSql = None
        self.delSql = None
        self.sqlReader = SqlReader(conf)
        
    
    def transmitConData(self, dumpType):
        exporter = Exporter(self.conf)
        pusher = Pusher(self.conf)
        
        if dumpType == SqlReader.FULL:
            sql = self.sqlReader.getConSql()
        elif dumpType == SqlReader.INCR:
            sql = self.sqlReader.getConIncSql()
        exporter.execute(sql)
        
        subResult = exporter.fetchmany()
        while subResult:
            pusher.pushConData(subResult)
            subResult = exporter.fetchmany()
            
        exporter.close()
        pusher.close()
    
    def transmitDelData(self, dumpType):
        exporter = Exporter(self.conf)
        pusher = Pusher(self.conf)
        
        if dumpType == SqlReader.FULL:
            sql = self.sqlReader.getDelSql()
        elif dumpType == SqlReader.INCR:
            sql = self.sqlReader.getDelIncSql()
        exporter.execute(sql)
        
        subResult = exporter.fetchmany()
        while subResult:
            pusher.pushDelData(subResult)
            subResult = exporter.fetchmany()
            
        exporter.close()
        pusher.close()
    
    def transmit(self, dumpType):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        self.transmitConData(dumpType)
        self.transmitDelData(dumpType)
        
        timestampFile = open(self.conf.timestampFile , "w")
        timestampFile.write(now)
        timestampFile.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        exit(1)
        
    type = sys.argv[1]
    serviceName = sys.argv[2]
    conf = imp.load_source("conf", serviceName + "/conf.py")
    transmitter = Transmitter(conf)
    
    if type == "full":
        dumpType = SqlReader.FULL
    elif type == "incr":
        dumpType = SqlReader.INCR
    else:
        print("type error")
        sys.exit(1)
    transmitter.transmit(dumpType)
