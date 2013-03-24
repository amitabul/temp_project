'''
Created on Mar 23, 2013

@author: amitabul
'''
import unittest
import sqlite3
from asap.exporter import Exporter
from test_collection import conf as TestConf

class ExporterTest(unittest.TestCase):

    def setUp(self):
        self.conn = None
        self.srcTableName = "test_collection"
        self.incSql = None
        
        
        sqlFile = open(TestConf.incSqlFile, "r")
        self.incSql = sqlFile.read()
        sqlFile.close()

        self.conn = sqlite3.connect("test.db", check_same_thread = False)
        cursor = self.conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS ' + self.srcTableName)
        cursor.execute(
                'CREATE TABLE ' 
                + self.srcTableName 
                + '(first TEXT, last TEXT)')
        data = [
            ("yungho", "yu"), 
            ("hojin", "kim"),
            ("kwanghee", "jang"),
        ]
        cursor.executemany(
                "INSERT INTO " + self.srcTableName + " values (?, ?)", data)
        self.conn.commit()

    def tearDown(self):
        cursor = self.conn.cursor()
        cursor.execute('DROP TABLE ' + self.srcTableName)
        self.conn.close()

    def testFetchmany(self):
        exporter = Exporter(TestConf)
        exporter.execute(self.incSql)
        
        result = []
        
        subResult = exporter.fetchmany()
        while subResult:
            for first, last in subResult:
                result.append((first, last))
            subResult = exporter.fetchmany()
        
        self.assertEqual(
                 "[('yungho', 'yu'), ('hojin', 'kim'), ('kwanghee', 'jang')]", 
                 str(result))
        
        exporter.close()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
