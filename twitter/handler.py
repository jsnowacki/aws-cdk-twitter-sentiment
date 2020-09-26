def lambda_handler(event, context):
     secret = get_secret(SECRET_NAME)
    public_tweets = api.home_timeline()
    for tweet in public_tweets:
        print(tweet, tweet.text)