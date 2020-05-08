from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .master('local')\
    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/covid_analytics.mobility_trends.coll')\
    .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/covid_analytics.mobility_trends.coll')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')\
    .getOrCreate()

df = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    .option("database","covid_analytics")\
    .option("collection", "mobility_trends")\
    .load()

print (df.show(n=2))
print ("data count" ,df.count())