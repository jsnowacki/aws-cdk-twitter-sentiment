#!/use/bin/env python
import json

import boto3
import base64
from botocore.exceptions import ClientError
from dataclasses import dataclass

import tweepy


@dataclass
class TwitterApiSecret:
    access_token: str
    access_token_secret: str
    api_key: str
    api_secret_key: str

def get_secret(secret_name: str, region_name: str = "eu-west-1") -> TwitterApiSecret:
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )


    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    
    secret = json.loads(get_secret_value_response['SecretString'])
        
    return TwitterApiSecret(**secret)

def get_tweepy_api(secret: TwitterApiSecret) -> tweepy.API:
    auth = tweepy.OAuthHandler(secret.api_key, secret.api_secret_key)
    auth.set_access_token(secret.access_token, secretpytes.access_token_secret)

    return tweepy.API(auth)
    