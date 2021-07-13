import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.appName('TwitterTask').getOrCreate()

file_path = 'data/tweets.json'

mentions_regex = '(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9_]+)'
hashtag_regex = '(?<=^|(?<=[^a-zA-Z0-9-_\.]))#([A-Za-z]+[A-Za-z0-9_]+)'


def get_mentions_udf(tweet):
    return re.findall(mentions_regex, tweet)


def get_hashtag_udf(tweet):
    return re.findall(hashtag_regex, tweet)


get_mentions = udf(lambda x: get_mentions_udf(x), ArrayType(StringType()))

get_hashtag = udf(lambda x: get_hashtag_udf(x), ArrayType(StringType()))

# Read JSON File
df_tweets = spark.read.json(file_path).rdd

# Find how many tweets each user has
df_tweets_count_user = df_tweets.map(lambda x: (x[4], 1)).reduceByKey(lambda x, y: x + y).sortBy(
    lambda x: -x[1])
df_tweets_count_user \
    .toDF(schema=['user', 'count']) \
    .repartition(1) \
    .write \
    .option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/rdd/tweet_count_by_user")

# Count how many times each person is mentioned
df_tweets_mentions = df_tweets.flatMap(lambda x: re.findall(mentions_regex, x[3]))

# Find the 10 most mentioned persons
df_tweets_mentions_only = df_tweets_mentions.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(
    lambda x: -x[1])
df_tweets_mentions_only \
    .toDF(schema=['mentions', 'count']) \
    .limit(10) \
    .repartition(1) \
    .write.option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/rdd/top_ten_mentions")

# Find all the hashtags mentioned on a tweet
df_tweets_hashtags = df_tweets.flatMap(lambda x: re.findall(hashtag_regex, x[3]))

# Find the 10 most popular Hashtags
df_tweets_hashtags_only = df_tweets_hashtags.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(
    lambda x: -x[1])
df_tweets_hashtags_only \
    .toDF(schema=['mentions', 'count']) \
    .limit(10) \
    .repartition(1) \
    .write.option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/rdd/top_ten_mentions")

# Find the top 5 countries which tweet the most.
df_tweets_by_country = df_tweets.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).sortBy(
    lambda x: -x[1])
df_tweets_by_country \
    .toDF(schema=['countries', 'count']) \
    .write \
    .option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/rdd/top_five_tweeting_countries")
