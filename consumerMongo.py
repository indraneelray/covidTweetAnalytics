from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads,dumps

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest', api_version=(0,1,0))
client = MongoClient('localhost:27017')
consumer.subscribe(['COVID19'])
collection = client.covid.covid
for message in consumer:
    msg = loads(message.value)
    collection.insert_one(msg)