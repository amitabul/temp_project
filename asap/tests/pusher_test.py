'''
Created on Mar 23, 2013

@author: amitabul
'''
import unittest
import mysql.connector
from mysql.connector import errorcode
import datetime, time

from asap.pusher import Pusher
from test_collection import conf as TestConf

class PusherTest(unittest.TestCase):

    def setUp(self):
        self.conn = mysql.connector.Connect(
                host = TestConf.dstHost,
                user = TestConf.dstUser,
                password = TestConf.dstPassword,
                database = TestConf.dstDatabase)
        cursor = self.conn.cursor()
        cursor.execute('TRUNCATE TABLE ' + TestConf.dstConTable)
        cursor.execute('TRUNCATE TABLE ' + TestConf.dstDelTable)
        
        now = datetime.datetime.now()
        self.data = [
                ('yungho', 'yu', now),
                ('hojin', 'kim', now),
                ('kwanghee', 'jang', now)
        ]

    def tearDown(self):
        self.conn.close()

    def testConPush(self):
        pusher = Pusher(TestConf)
        pusher.pushConData(self.data)
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM ' + TestConf.dstConTable)
        rows = cursor.fetchall()
        self.assertEqual(3, len(rows))

    def testDelPush(self):
        pusher = Pusher(TestConf)
        pusher.pushDelData(self.data)
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM ' + TestConf.dstDelTable)
        rows = cursor.fetchall()
        self.assertEqual(3, len(rows))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()