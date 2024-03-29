'''
Created on Mar 24, 2013

@author: amitabul
'''
import unittest
import sqlite3
import mysql.connector
import datetime 

from asap.transmitter import Transmitter
from test_collection import conf as TestConf
from asap.sql_reader import SqlReader

class TransmitterTest(unittest.TestCase):

    def setUp(self):
        self.conn = None
        self.srcTableName = "test_collection"
        self.conSql = None
        self.data = None
        self.dstConn = None
        
        
        incSqlFile = open(TestConf.conSqlFile, "r")
        self.conSql = incSqlFile.read()
        incSqlFile.close()

        self.conn = sqlite3.connect("test.db", check_same_thread = False)
        srcCursor = self.conn.cursor()
        srcCursor.execute('DROP TABLE IF EXISTS ' + self.srcTableName)
        srcCursor.execute(
                'CREATE TABLE ' 
                + self.srcTableName 
                + '(first TEXT, last TEXT, time DATETIME)')
        now = datetime.datetime.now()
        self.data = [
            ("yungho", "yu", now), 
            ("hojin", "kim", now),
            ("kwanghee", "jang", now),
        ]
        srcCursor.executemany(
                "INSERT INTO " + self.srcTableName + " values (?, ?, ?)", self.data)
        self.conn.commit()
        self.conn.close()
        
        self.dstConn = mysql.connector.Connect(
                host = TestConf.dstHost,
                user = TestConf.dstUser,
                password = TestConf.dstPassword,
                database = TestConf.dstDatabase)
        dstCursor = self.dstConn.cursor()
        dstCursor.execute('TRUNCATE TABLE ' + TestConf.dstConTable)
        dstCursor.execute('TRUNCATE TABLE ' + TestConf.dstDelTable)
        self.dstConn.commit()

    def tearDown(self):
        self.dstConn.close()

    def testConTransmit(self):
        transmitter = Transmitter(TestConf)
        transmitter.transmitConData(SqlReader.FULL)
        
        dstCursor = self.dstConn.cursor()
        dstCursor.execute('select * from ' + TestConf.dstConTable)
        rows = dstCursor.fetchall()
        self.assertEqual(len(self.data), len(rows))
        
    def testDelTransmit(self):
        transmitter = Transmitter(TestConf)
        transmitter.transmitDelData(SqlReader.FULL)
        
        dstCursor = self.dstConn.cursor()
        dstCursor.execute('select * from ' + TestConf.dstDelTable)
        rows = dstCursor.fetchall()
        self.assertEqual(len(self.data), len(rows))
        
        
    def testTimestamp(self):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        transmitter = Transmitter(TestConf)
        transmitter.transmit(SqlReader.FULL)
        
        file = open(TestConf.timestampFile)
        fileTime = file.read()
        self.assertEqual(now, fileTime)
        file.close()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testTransmit']
    unittest.main()