from pymongo import MongoClient

def setupdb():
    temp="mongodb+srv://Admin:adminpassword@cluster0.z4bjwnd.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(temp)
    return client["TESTDB"]




