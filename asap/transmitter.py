'''
Created on Mar 24, 2013

@author: amitabul
'''

import datetime
from asap.exporter import Exporter
from asap.pusher import Pusher

class Transmitter():
    
    def __init__(self, conf):
        self.conf = conf
        self.incSql = None
        self.delSql = None
        
    def readSql(self, filePath):
        sql = None
        file = open(filePath, "r")
        sql = file.read()
        file.close()
        return sql
    
    def getIncSql(self):
        if self.incSql == None :
            self.incSql = self.readSql(self.conf.incSqlFile)
        return self.incSql
    
    def getDelSql(self):
        if self.delSql == None :
            self.delSql = self.readSql(self.conf.delSqlFile)
        return self.delSql
    
    def transmitData(self, sql):
        exporter = Exporter(self.conf)
        pusher = Pusher(self.conf)
        
        exporter.execute(sql)
        
        subResult = exporter.fetchmany()
        while subResult:
            pusher.pushIncData(subResult)
            subResult = exporter.fetchmany()
            
        exporter.close()
        pusher.close()
    
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