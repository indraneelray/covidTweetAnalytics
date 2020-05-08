# ----------------------------------------------------------------------------#
# Imports
# ----------------------------------------------------------------------------#
import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
import configparser

# ----------------------------------------------------------------------------#
# App Config.
# ----------------------------------------------------------------------------#
parser = configparser.RawConfigParser()   
parser.read_file(open(r'./conf/dbConfig.conf'))
mobility_trend_path = parser.get('database-config', 'MOBILITY_TREND_PATH')

print(mobility_trend_path)

csvfile = open(mobility_trend_path.replace('"', ''), 'r')
reader = csv.DictReader(csvfile)
mongo_client=MongoClient()

db=mongo_client.covid
db.segment.drop()

header= [ "iso","country","date","grocery_pharmacy","parks",\
    "residential","retail_recreation","transit_stations",\
        "workplaces","total_cases","fatalities"]

for each in reader:
    row={}
    for field in header:
        row[field]=each[field]

    db.mobility_trends.insert(row)