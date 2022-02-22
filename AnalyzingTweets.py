from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, split
import pyspark.sql.functions as sql_fun


if __name__ == "__main__":

    # create Spark session
    spark = SparkSession.builder.appName("TwitterAnalysis").getOrCreate()

    # read the tweet data from socket
    tweet_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "127.0.0.1") \
        .option("port", 3339) \
        .load()

    # type cast the column value
    tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")

    # filter tweets which are containing music as key word
    music_tweets = tweet_df_string.filter(sql_fun.lower(tweet_df_string.value).contains("music"))
    
    # Produce a count of all tweets consumed
    tweets_count = music_tweets.count()
    
    # Produce a count of unique tweets 
    unique_tweets_count = music_tweets.agg(countDistinct("value"))
    
    # write the above data into memory. Store the tweets into a database of your choosing. and let the trigger runs in every 2 secs.
    writeTweet = music_tweets.writeStream. \
        outputMode("complete"). \
        format("memory"). \
        queryName("justinTweets"). \
        trigger(processingTime='2 seconds'). \
        start()
    
    print("----- streaming is running -------")
    
    
