import os
import json
import base64
import boto3
from typing import Iterable, List

from tweepy import Status

from api import get_secret, get_tweepy_api

class FirehoseRecordPutException(Exception): pass

SECRET_NAME = os.environ['SECRET_NAME']
DELIVERY_STREAM_NAME = os.environ['DELIVERY_STREAM_NAME']
_secret = get_secret(SECRET_NAME)
_api = get_tweepy_api(_secret)

def to_firehose_records(tweets: Iterable[Status]) -> List[bytes]:
    return [
        {'Data': base64.b64encode(json.dumps(tweet._json).encode('utf-8'))}
        for tweet in tweets
    ]

def lambda_handler(event, context):
    public_tweets = _api.home_timeline(tweet_mode='extended')
    client = boto3.client('firehose')
    resp = client.put_record_batch(
        DeliveryStreamName=DELIVERY_STREAM_NAME,
        Records=to_firehose_records(public_tweets)
    )

    if resp['FailedPutCount'] > 0:
        msg = f"Failed to put {resp['FailedPutCount']} records: {resp['RequestResponses']}"
        raise FirehoseRecordPutException(msg)

    return '200 OK'