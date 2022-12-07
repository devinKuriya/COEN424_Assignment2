from pymongo import MongoClient

#Code format from MongoDB documentation
def setupdb():
    CONNECTION_STRING="mongodb+srv://Admin:adminpassword@cluster0.z4bjwnd.mongodb.net/Cluster0?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)
    temporary=client["Cluster0"]
    return temporary


if __name__=='__main__':
    dbname = setupdb()

