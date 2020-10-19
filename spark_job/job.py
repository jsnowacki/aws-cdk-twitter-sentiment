import sys

import pyspark
from awsglue.context import GlueContext, DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

from transform import transform_tweets

sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'input_s3',
                           'glue_database',
                           'glue_table',
                           ])

tweets = spark.read.json(args['input_s3'])
tweets_cleaned = transform_tweets(tweets)

glue.write_dynamic_frame_from_catalog(
    frame=DynamicFrame.fromDF(tweets_cleaned, glue, 'tweets_cleaned'),
    database=args['glue_database'],
    table_name=args['glue_table'],
    transformation_ctx='write_sink'
)
