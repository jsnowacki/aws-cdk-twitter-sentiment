import sys

import pyspark
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from transform import transform_tweets

sc = SparkContext.getOrCreate()
glue = GlueContext(sc).getOrCreate()
spark = glue.spark_session

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'input_path',
                           'output_table',
                           ])


tweets = spark.read.json(args['input_path'])
tweets_cleaned = transform_tweets(tweets)

