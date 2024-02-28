import tweepy 
import pandas as pd
import json 
from datetime import datetime
import s3fs


def _run_twitter_etl() :
    
    access_key = "your_api_key"
    access_secret = "your_api_secret_key"
    consumer_key = "access_token_key"
    consumer_secret = "access_token_secret_key"

    # Twitter authentifiocation 
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    # Creating an API object 
    api = tweepy.API(auth)

    ## If you have an Pro APi acess
    tweets = api.user_timeline(screen_name ='@elonmusk',
                              count = 200,
                              include_rts = False,
                              tweet_mode = 'extended')
    tweets.to_csv("s3://airflow-etl-twitter/elon_musk_twitter.csv")
    
    return(True)

## if ypu don't have an access api 
def run_twitter_etl() :
    
    tweets = pd.read_csv("./tweets.csv")
    tweets.to_csv("s3://airflow-etl-twitter/elon_musk_twitter.csv")
    
    return (True)



