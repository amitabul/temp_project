'''
Created on Mar 24, 2013

@author: amitabul
'''
import imp
import unittest

from asap.sql_reader import SqlReader

class SqlReaderTest(unittest.TestCase):

    def setUp(self):
        import test_collection.conf as TestConf
        self.conf = TestConf
    
    def tearDown(self):
        pass

    def testGetConIncSql(self):
        sqlReader = SqlReader(self.conf)
        sql = sqlReader.getConIncSql("2013-03-24 10:56:00")
        
        self.assertEqual(
                 "SELECT * FROM test_collection " +
                     "WHERE datetime > 2013-03-24 10:56:00", sql)
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()