import pyspark
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def transform_tweets(tweets: DataFrame) -> DataFrame:
    # "Sat Oct 17 18:30:09 +0000 2020"
    result = tweets.select([
        'id', 
        F.to_timestamp('created_at', 'EEE MMM dd HH:mm:ss +0000 yyyy').alias('created_at'), 
        'full_text', 
        'lang'
        ])\
        .withColumn('year', F.year('created_at'))\
        .withColumn('month', F.month('created_at'))\
        .withColumn('day', F.dayofmonth('created_at'))

    return result


if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    tweets = spark.read.json('sample_tweets.json')
    tweets_cleaned = transform_tweets(tweets)
    tweets_cleaned.show()
    tweets_cleaned.printSchema()
