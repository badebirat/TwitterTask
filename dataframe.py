import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName('TwitterTask').getOrCreate()

file_path = 'data/tweets.json'
output_file_path = 'output/dataframe'

mentions_regex = '(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9_]+)'
hashtag_regex = '(?<=^|(?<=[^a-zA-Z0-9-_\.]))#([A-Za-z]+[A-Za-z0-9_]+)'


def get_mentions_udf(tweet):
    return re.findall(mentions_regex, tweet)


def get_hashtag_udf(tweet):
    return re.findall(hashtag_regex, tweet)


get_mentions = udf(lambda x: get_mentions_udf(x), ArrayType(StringType()))

get_hashtag = udf(lambda x: get_hashtag_udf(x), ArrayType(StringType()))

# Read JSON File
df_tweets = spark.read.json(file_path)

# Find how many tweets each user has
df_tweets_count_by_user = df_tweets.groupBy('user').count().sort(col('count').desc())
df_tweets_count_by_user \
    .repartition(1) \
    .write \
    .option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/tweet_count_by_user")

# Find all the persons mentioned on tweets
df_tweets_mentions = df_tweets.withColumn('mentions', get_mentions('text'))
df_tweets_mentions_only = df_tweets_mentions.select(F.explode(df_tweets_mentions.mentions))
df_tweets_mentions_only \
    .repartition(1) \
    .write.option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/all_mentions")

# Count how many times each person is mentioned
df_tweets_mentions_only_count = df_tweets_mentions_only.groupBy('col').count().sort(col('count').desc())
df_tweets_mentions_only_count \
    .repartition(1) \
    .write \
    .option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/mentions_count")

# Find the 10 most mentioned persons
df_tweets_mentions_only_top_ten = df_tweets_mentions_only.groupBy('col').count().sort(
    col('count').desc()).limit(10)
df_tweets_mentions_only_top_ten \
    .write \
    .option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/top_ten_mentions")

# Find all the hashtags mentioned on a tweet
df_tweets_hashtag = df_tweets.withColumn('hashtag', get_hashtag('text'))
df_tweets_hashtag_only = df_tweets_hashtag.select(F.explode(df_tweets_hashtag.hashtag))
df_tweets_hashtag_only \
    .repartition(1) \
    .write \
    .option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/all_hashtags")

# Count how many times each hashtag is mentioned
df_tweets_hashtag_only_count = df_tweets_hashtag_only.groupBy('col').count().sort(col('count').desc())
df_tweets_hashtag_only_count \
    .repartition(1) \
    .write.option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/hashtag_count")

# Find the 10 most popular Hashtags
df_tweets_hashtag_only_top_ten = df_tweets_hashtag_only.groupBy('col').count().sort(col('count').desc()).limit(10)
df_tweets_hashtag_only_top_ten \
    .write \
    .option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/top_ten_hashtags")

# Find the top 5 countries which tweet the most.
df_tweets_by_country = df_tweets.groupBy('country').count().sort(col('count').desc()).limit(5)
df_tweets_by_country \
    .write.option("header", "true") \
    .option("sep", ",") \
    .format("com.databricks.spark.csv") \
    .mode("overwrite") \
    .csv("output/dataframe/top_five_tweeting_countries")
