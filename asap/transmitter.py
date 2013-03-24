'''
Created on Mar 24, 2013

@author: amitabul
'''
import imp
import sys
import datetime
from asap.exporter import Exporter
from asap.pusher import Pusher

class Transmitter():
    
    def __init__(self, conf):
        self.conf = conf
        self.incSql = None
        self.delSql = None
        
    def readSql(self, filePath):
        file = open(filePath, "r")
        sql = file.read()
        file.close()

        # TODO: TIMESTAMP 보고 sql 수정해서 return 해주자.
        return sql
    
    def getIncSql(self):
        if self.incSql == None :
            self.incSql = self.readSql(self.conf.incSqlFile)
        return self.incSql
    
    def getDelSql(self):
        if self.delSql == None :
            self.delSql = self.readSql(self.conf.delSqlFile)
        return self.delSql
    
    def transmitIncData(self):
        exporter = Exporter(self.conf)
        pusher = Pusher(self.conf)
        
        exporter.execute(self.getIncSql())
        
        subResult = exporter.fetchmany()
        while subResult:
            pusher.pushIncData(subResult)
            subResult = exporter.fetchmany()
            
        exporter.close()
        pusher.close()
    
    def transmitDelData(self):
        exporter = Exporter(self.conf)
        pusher = Pusher(self.conf)
        
        exporter.execute(self.getDelSql())
        
        subResult = exporter.fetchmany()
        while subResult:
            pusher.pushDelData(subResult)
            subResult = exporter.fetchmany()
            
        exporter.close()
        pusher.close()
    
    def transmit(self):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.transmitIncData()
        self.transmitDelData()
        
        timestampFile = open(self.conf.serviceHome + "/TIMESTAMP" , "w")
        timestampFile.write(now)
        timestampFile.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        exit(1)
        
    serviceName = sys.argv[1]
    conf = imp.load_source("conf", serviceName + "/conf.py")
    transmitter = Transmitter(conf)
    transmitter.transmit()
