import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf

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
df_tweets = spark.read.json(file_path)

# Find how many tweets each user has
df_tweets_count_by_user = df_tweets.groupBy('user').count().sort(col('count').desc())

# Find all the persons mentioned on tweets
df_tweets_mentions = df_tweets.withColumn('mentions', get_mentions('text'))
df_tweets_mentions_only = df_tweets_mentions.select(F.explode(df_tweets_mentions.mentions))

# Count how many times each person is mentioned
df_tweets_mentions_only_count = df_tweets_mentions_only.groupBy('col').count().sort(col('count').desc())

# Find the 10 most mentioned persons
df_tweets_mentions_only_top_ten = df_tweets_mentions_only.groupBy('col').count().sort(
    col('count').desc()).limit(10)

# Find all the hashtags mentioned on a tweet
df_tweets_hashtag = df_tweets.withColumn('hashtag', get_hashtag('text'))
df_tweets_hashtag_only = df_tweets_hashtag.select(F.explode(df_tweets_hashtag.hashtag))

# Count how many times each hashtag is mentioned
df_tweets_hashtag_only_count = df_tweets_hashtag_only.groupBy('col').count().sort(col('count').desc())

# Find the 10 most popular Hashtags
df_tweets_hashtag_only_top_ten = df_tweets_hashtag_only.groupBy('col').count().sort(col('count').desc()).limit(10)

# Find the top 5 countries which tweet the most.
df_tweets_by_country = df_tweets.groupBy('country').count().sort(col('count').desc()).limit(5)
