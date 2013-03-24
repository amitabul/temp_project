'''
Created on Mar 24, 2013

@author: amitabul
'''

import re

class SqlReader(object):
    '''
    classdocs
    '''
    
    FULL, INCR = range(2)
    
    def __init__(self, conf):
        '''
        Constructor
        '''
        self.conf = conf
        self.conSql = None
        self.delSql = None
        self.conIncSql = None
        self.delIncSql = None
        
    def readSql(self, filePath):
        file = open(filePath, "r")
        sql = file.read()
        file.close()

        # TODO: TIMESTAMP 보고 sql 수정해서 return 해주자.
        return sql
    
    def getConSql(self):
        if self.conSql == None :
            self.conSql = self.readSql(self.conf.conSqlFile)
        return self.conSql
    
    def getDelSql(self):
        if self.delSql == None :
            self.delSql = self.readSql(self.conf.delSqlFile)
        return self.delSql
    
    def getConIncSql(self, timestamp):
        oriSql = self.getConSql()
        
        def makeWhere(matchObj):
            return re.sub(r"(\$TIMESTAMP)", timestamp, matchObj.group(1))
        
        return re.sub("/\*{{(.*)}}\*/", makeWhere, oriSql)
        