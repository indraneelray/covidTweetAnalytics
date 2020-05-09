from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("myApp")\
    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/covid.mobility_trends.coll')\
    .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/covid.job_collections.coll')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.0')\
    .getOrCreate()

df = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    .option("database","covid")\
    .option("collection", "mobility_trends")\
    .load()

df = df.groupBy("country_region").count()

df.write\
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .option("collection", "job_collections") \
    .save()