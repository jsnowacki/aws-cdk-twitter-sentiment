import pytest
from pyspark.sql import SparkSession
from datetime import datetime

from transform import transform_tweets

@pytest.fixture(scope='module')
def spark():
    return ( 
        SparkSession
            .builder
            .appName('twitter-spark-job-test')
            .master('local[*]')
            .getOrCreate()
    )

def test_transform(spark: SparkSession):
    tweets = spark.read.json('sample_tweets.json')
    tweets_cleaned = transform_tweets(tweets)

    result = tweets_cleaned.collect()

    assert len(result) > 0

    expected_columns = {'id', 'created_at', 'full_text', 'lang' ,'year', 'month', 'day'}
    result_columns = set(tweets_cleaned.columns)

    assert len(expected_columns - result_columns) == 0

    for row in result:
        assert row['id'] > 0
        assert len(row['full_text']) > 0
        assert len(row['lang']) == 2
        assert type(row['created_at']) == datetime

