import os

from api import get_secret, get_tweepy_api

SECRET_NAME = os.environ['SECRET_NAME']
_secret = get_secret(SECRET_NAME)
_api = get_tweepy_api(_secret)

def lambda_handler(event, context):
    public_tweets = _api.home_timeline()
    for tweet in public_tweets:
        print(tweet, tweet.text)