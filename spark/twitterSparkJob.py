from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("myApp")\
    .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/covid.covid.coll')\
    .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/covid.coivid.coll')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.0')\
    .getOrCreate()

df = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    .option("database","covid")\
    .option("collection", "covid")\
    .load()

df.createOrReplaceTempView("tweets")

finalTweetOutput = spark.sql("select * from tweets where user.verified = True")

finalTweetOutput.write\
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .option("collection", "verifiedTweets") \
    .save()

finalTweetOutput2 = spark.sql("select * from tweets where user.verified = False")
print("tweet count", finalTweetOutput2.count())

finalTweetOutput2.write\
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .option("collection", "nonVerifiedTweets") \
    .save()


