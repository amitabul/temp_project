'''
Created on Mar 23, 2013

@author: amitabul
'''

import mysql.connector
from mysql.connector import errorcode

class Pusher(object):
    '''
    classdocs
    '''


    def __init__(self, conf):
        '''
        Constructor
        '''
        self.conf = conf
        self.conn = None
    
    def connect(self):
        self.conn = mysql.connector.Connect(
                host = self.conf.dstHost,
                user = self.conf.dstUser,
                password = self.conf.dstPassword,
                database = self.conf.dstDatabase)
    def getConn(self):
        if self.conn == None :
            self.connect()
        return self.conn
        
    def close(self):
        self.conn.close()
    
    def pushIncData(self, data):
        conn = self.getConn()
        cursor = conn.cursor()
        cursor.executemany(self.makeIncStmt(), data)
        conn.commit()
        
    def pushDelData(self, data):
        conn = self.getConn()
        cursor = conn.cursor()
        cursor.executemany(self.makeDelStmt(), data)
        conn.commit()
        
    def __getVarStr(self, str):
        varStr = ''
        for _ in range(0, len(self.conf.dstIncFields.split(','))):
            varStr = varStr + ",%s"
        return varStr.lstrip(',')
    
    def makeIncStmt(self):
        varStr = self.__getVarStr(self.conf.dstIncFields)
        return "INSERT INTO " + self.conf.dstIncTable + \
            "(" + self.conf.dstIncFields + ") VALUES (" + varStr + ')'
            
    def makeDelStmt(self):
        varStr = self.__getVarStr(self.conf.dstDelFields)
        return "INSERT INTO " + self.conf.dstDelTable + \
            "(" + self.conf.dstDelFields + ") VALUES (" + varStr + ')'