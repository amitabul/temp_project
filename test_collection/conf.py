'''
Created on Mar 23, 2013

@author: amitabul
'''

import os 

serviceHome = os.path.dirname(__file__)
sqlDir = serviceHome + '/sqls'

srcAddress = "test.db"
fetchsize = 1000
incSqlFile = sqlDir + "/export.sql"
delSqlFile = sqlDir + "/delete.sql"

dstHost = "127.0.0.1"
dstUser = "search"
dstPassword = "1234"
dstDatabase = "rawdata"
dstIncTable = "test_collection"
dstIncFields = "firstname, lastname, datetime"
dstDelTable = "test_del_collection"
dstDelFields = "firstname, lastname, datetime"
