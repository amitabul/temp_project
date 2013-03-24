'''
Created on Mar 23, 2013

@author: amitabul
'''

import sqlite3

class Exporter:
    def __init__(self, conf):
        self.conf = conf
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(
                    self.conf.srcAddress, 
                    check_same_thread = False)
        except sqlite3.Error as e:
            print("An error occurred:" +  e.args[0])
            raise e
    
    def getConn(self):
        if self.conn == None :
            self.connect()
        return self.conn
        
    def close(self):
        self.conn.close()

    def execute(self, sql):
        self.cursor = self.getConn().cursor()
        self.cursor.execute(sql)

    def fetchall(self):
        return self.cursor.fetchall()
    
    def fetchmany(self):
        return self.cursor.fetchmany(self.conf.fetchsize)

