from pymongo import MongoClient
import json

class MongoDBUtility:
    def __init__(self, database_name, collection_name):
        self.conn = MongoClient('localhost:27017')
        self.db = self.conn[database_name]
        self.collection = self.db[collection_name]

    def getMongoDBConn(self):
        return self.conn

    def insertMultipleRecords(self,obj_to_write):
        x = self.collection.insert_many(obj_to_write)
        return x.inserted_ids

    def insertOneRecord(self, obj_to_write):
        x = self.collection.insert_one(obj_to_write)
        return x.inserted_id