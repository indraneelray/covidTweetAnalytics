import re

import sparknlp
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from sparknlp.pretrained import PretrainedPipeline
from pymongo import MongoClient


spark = SparkSession.builder \
    .appName("Spark NLP")\
    .master("local[4]")\
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "2G") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.0")\
    .config("spark.kryoserializer.buffer.max", "1000M")\
    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/covid.covid.coll')\
    .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/covid.coivid.coll')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.0')\
    .config('spark.jars.packages','com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.5')\
    .getOrCreate()

##read from mongodb into pyspark dataframe
twitter_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","covid").option("collection", "covid").load()
print(twitter_df.show(n=2))


#input=twitter_df.select('id','text').toDF("id","tweet_text")
#print("imput")
#input.show()

##preprocess data - remove hashtags, punctutations , hyperlinks, @symbols,

def cleantext(text):
    text = re.sub('@[A-Za-z0–9]+', '', text)  # Removing @mentions
    text = re.sub('#', '', text)  # Removing '#' hash tag
    text = re.sub('RT[\s]+', '', text)  # Removing RT
    text = re.sub('https?:\/\/\S+', '', text)  # Removing hyperlink
    text = re.sub(':', '', text) #remove colon
    text=re.sub('(\.|\!|\?|\,)', '', text) #remove punctutations
    return text

udf_fun = udf(lambda text:cleantext(text),StringType())
preprocessed_text = twitter_df.select('id',udf_fun('text').alias('text'), 'user')

preprocessed_text.show()

#use pipeline
pipeline = PretrainedPipeline("analyze_sentiment")
result = pipeline.annotate(preprocessed_text,column='text')
#result.select("sentiment.result").show()

#write result to mongodb

cols = ['id','text','sentiment.result', 'user']
output = result.select(cols)
#output.show()


output.write\
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .option("collection", "sentiment_predicted") \
    .save()