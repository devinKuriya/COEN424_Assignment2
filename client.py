import json
from dbconnect import setupdb
import pandas as ps

def setcollections():
    #Read through csv files
    datadvd_test = ps.read_csv('DVD-testing.csv')
    datadvd_training = ps.read_csv('DVD-training.csv')
    dataNDB_testt = ps.read_csv('NDBench-testing.csv')
    dataNDB_training = ps.read_csv('NDBench-training.csv')

    dvd_test_to_json = datadvd_test.to_json(orient='records')
    payloaddatadvd_test = json.loads(dvd_test_to_json)
    dvd_test.insert_many(payloaddatadvd_test)

    datadvd_training_json=datadvd_training.to_json(orient='records')
    payloaddatadvd_training = json.loads(datadvd_training_json)
    dvd_training.insert_many(payloaddatadvd_training)

    # datandb_testing_json=dataNDB_testt.to_json(orient='records')
    # payloaddatandb_testing_json = json.loads(datandb_testing_json)
    # NDB_test.insert_many(payloaddatandb_testing_json)

    # datandb_training_json=dataNDB_training.to_json(orient='records')
    # payloaddatandb_training_json = json.loads(datandb_training_json)
    # NDB_training.insert_many(payloaddatandb_training_json)

def updatecollection(collectionname,benchmarktype,datatype):
    dbname[collectionname].update_many({},
    [
        {"$set": { "benchmarktype": benchmarktype, "datatype": datatype} },
        {"$unset": ["Final_Target"] }
    ])

def allcollections(collectionname,total):
    values=dbname[collectionname].find()
    for i in values:
        dbname[total].insert_one(i)



dbname= setupdb()
#Set Collections
# dvd_test=dbname["DVD-testing"]
# dvd_training=dbname["DVD-training"]
# NDB_test=dbname["NDB-testing"]
# NDB_training=dbname["NDB-training"]
# total=dbname["FullDataSet"]

#Used to update datasets with attributs to implemeent match
# updatecollection("DVD-testing","DVD","testing")
# updatecollection("DVD-training","DVD","training")
# updatecollection("NDB-testing","NDB","testing")
# updatecollection("NDB-training","NDB","training")

#Function used to add data to database
#setcollections()

#Function used to aggreagate all datasets into 1
# allcollections("NDB-testing","FullDataSet")


#To delete colleciton
#collection.delete_many({})


#Get user input
id=input('Please input an ID(if you wish to close client click enter): \n')
BenchmarkType=input('Please input a BenchmarkType(DVD or NDB): \n')
WorkloadMetric=input('Please input a WorkloadMetric: \n')
BatchUnit=input('Please input a BatchUnit: \n')
BatchID=input('Please input a BatchID: \n')
BatchSize=input('Please input a BatchSize: \n')
DataType=input('Please input a DataType(training or testing): \n')

#collection_name=BenchmarkType+"-"+DataType
database_collection=dbname["FullDataSet"]

#Calculate start and end lines
linestart=(int(BatchUnit)*(int(BatchID)-1))
lineend=(int(BatchUnit)*(int(BatchSize)))
# print(linestart)#used for testing
# print(lineend)#used for testing

#Create pipeline to caclualte analytics
pipeline=[
    {
        "$match":{"benchmarktype":BenchmarkType,"datatype":DataType}#Match datatype and benchmark type
    },
    {
        "$skip":linestart#skip number of rows you want
    },
    {
         "$limit":lineend#limt of data points
    },
    { "$sort": { WorkloadMetric: 1 } },#Sort the data
    {
        #Calculate avg, max, min and std.
        "$group":{
            "_id":WorkloadMetric,
            "average":{"$avg":"$"+WorkloadMetric},
            "max":{"$max":"$"+WorkloadMetric},
            "min":{"$min":"$"+WorkloadMetric},
            "std":{"$stdDevSamp":"$"+WorkloadMetric},
            "valueArray": {"$push": "$"+WorkloadMetric }#Save values to an array
        }
    },
    #Start Median calculatipons
    #Find size of array
    {
    "$project": {
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "size1": { "$size": [ "$valueArray" ] }
    }
    },
    #Finding if array has even or odd number of values
    {
    "$project": {
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "size1":1,
        "valueArray": 1,
        "remainder":{"$mod":["$size1",2]}
    }
    },
    #Finding if array has even or odd number of values
    {
     "$project": {
        "remainder":1,
        "max":1,
        "min":1,
        "std":1,
        "size1":1,
        "average":1,
        "valueArray": 1,
        "len":{"$cond":{"if":{"$eq":["$remainder",0]},"then":0, "else":1}}
    }
    },
    #Find size -1 since index starts at 0
    {
    "$project": {
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "len":1,
        "size1":1,
        "fixedlength":{"$subtract":["$size1",1]}
    }
    },
    #Find the middle index
    {
    "$project": {
        "fixedlength":1,
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "len":1,
        "size1":1,
        "middle_item_index":{"$trunc":{"$divide":["$fixedlength",2]}}
    }
    },
    #Find value at middle or left middle
    {
    "$project": {
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "len":1,
        "size1":1,
        "middle_item_index":1,
        "middle":1,
        "middle_of_array":{"$arrayElemAt":["$valueArray","$middle_item_index"]}
    }},
    #Find the item index 1 to the right
    {
    "$project": {
        "middle_of_array":1,
        "middle_item_index":1,
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "len":1,
        "size1":1,
        "seconditem":{"$add":["$middle_item_index",1]}
    }
    },
    #Find the item value 1 to the right
    {
       "$project": {
        "middle_of_array":1,
        "middle_item_index":1,
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "seconditem":1,
        "len":1,
        "size1":1,
         "middle_of_array2":{"$arrayElemAt":["$valueArray","$seconditem"]}
    }
    },
    #If length=even, we need to add the 2 middle items to later divide by 2
    {
    "$project": {
        "middle_of_array":1,
        "middle_of_array2":1,
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "len":1,
        "size":1,
        "evenarraymiddlepointadded":{"$add":["$middle_of_array","$middle_of_array2"]}
    }
    },
    #Divide by 2
    {
    "$project": {
        "evenarraymiddlepointadded":1,
        "middle_of_array":1,
        "middle_of_array2":1,
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "len":1,
        "size":1,
         "evenMedianValue":{"$divide":["$evenarraymiddlepointadded",2]}
    }
    },
    #Calculate and set median value
    {
    "$project": {
        "evenMedianValue":1,
        "middle_of_array":1,
        "middle_of_array2":1,
        "len":1,
        "max":1,
        "min":1,
        "std":1,
        "average":1,
        "valueArray": 1,
        "len":1,
        "size":1,
        "median":{"$cond":{ "if":{"$eq":["$len",0]},"then":"$evenMedianValue","else":"$middle_of_array"}}
    }
    },
    #All needed values 
    {
    "$project": {
        "valueArray": 1,
        "median":1,
        "max":1,
        "min":1,
        "std":1,
        "average":1,
    }
    }
]
#Run aggregation
run=database_collection.aggregate(pipeline)
for entries in run:
    print(entries)
#database_collection.aggregate(pipeline).explain("exectionStats")
