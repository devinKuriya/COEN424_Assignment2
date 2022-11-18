import json
from dbconnect import setupdb
import pandas as ps


mydb= setupdb()
#Set Collections
dvd_test=mydb["DVD-testing"]
dvd_training=mydb["DVD-training"]
NDB_test=mydb["NDB-testing"]
NDB_test=mydb["NDB-training"]
fulldb=mydb["fulldb"]

#Read through csv files
datadvd_test = ps.read_csv('DVD-testing.csv')
datadvd_training = ps.read_csv('DVD-training.csv')
dataNDB_testt = ps.read_csv('NDBench-testing.csv')
dataNDB_training = ps.read_csv('NDBench-training.csv')

payloaddatadvd_test = json.loads(datadvd_test.to_json(orient='records'))
payloaddatadvd_training = json.loads(datadvd_training.to_json(orient='records'))
payloaddataNDB_testt = json.loads(dataNDB_testt.to_json(orient='records'))
payloaddataNDB_training = json.loads(dataNDB_training.to_json(orient='records'))


datadvd_test.insert_many(payloaddatadvd_test)
datadvd_training.insert_many(payloaddatadvd_training)
dataNDB_testt.insert_many(payloaddataNDB_testt)
dataNDB_training.insert_many(payloaddataNDB_training)

