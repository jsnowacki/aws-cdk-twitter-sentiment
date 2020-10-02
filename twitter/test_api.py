from api import get_secret, get_tweepy_api, TwitterApiSecret
import json

SECRET_NAME = "TwitterAPIKeys"

def test_get_secret():
    secret = get_secret(SECRET_NAME)
    assert secret is not None
    assert type(secret) is TwitterApiSecret
    assert len(secret.access_token) > 0
    assert len(secret.access_token_secret) > 0
    assert len(secret.api_key) > 0
    assert len(secret.api_secret_key) > 0

def test_get_twitter_api():
    secret = get_secret(SECRET_NAME)
    api = get_tweepy_api(secret)
    assert secret is not None
    assert api is not None
    public_tweets = api.home_timeline(tweet_mode="extended")
    for tweet in public_tweets:
        assert len(tweet.full_text) > 0
        assert len(json.dumps(tweet._json)) > 0
