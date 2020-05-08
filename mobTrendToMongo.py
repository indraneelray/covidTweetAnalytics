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

header= [ "country_region_code","country_region","sub_region_1","sub_region_2","date",\
    "retail_and_recreation_percent_change_from_baseline","grocery_and_pharmacy_percent_change_from_baseline",\
        "parks_percent_change_from_baseline","transit_stations_percent_change_from_baseline",\
            "workplaces_percent_change_from_baseline","residential_percent_change_from_baseline"]

for each in reader:
    row={}
    for field in header:
        row[field]=each[field]

    db.mobility_trends.insert(row)