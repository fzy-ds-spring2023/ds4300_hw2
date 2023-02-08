"""
Felix Yang, Ayushi Shirke
Run API calls to insert into database and retrieve home timelines
Using Python time library to record time
"""
from redis_tweet_api import RedisTwitterAPI_1, RedisTwitterAPI_2
from tweet_object import Tweet
import pandas as pd
import random
import time

def run_tweet(api):

    # Authenticate
    api = api()
    
    #generate follows table
    follow = pd.read_csv("data/follows.csv")
    api.generate_follows_from_dataframe(follow)
    
    # insert tweets (1,000,000 / time --> inserts/sec)
    start = time.time()
    df_tweet = pd.read_csv("data/tweet.csv")
    
    # get Tweet object from row
    for idx, row in df_tweet.iterrows():
        api.post_tweet(idx, Tweet(user=row[0], ts=time.time(), text=row[1]))
    total_time = time.time()-start
    print(f'Time to post tweets = {total_time} seconds')
    print(f'-> {df_tweet.shape[0]/total_time} api calls/second')
    
    # get distinct users
    users = api.get_users()

    # get timelines (5000 / time --> timelines/sec)
    timelines = []
    n = 5000
    before = time.time()
    for _ in range(n):
        user = random.choice(users) # randomize from distinct user list, then call api
        timelines.append(api.get_user_timeline(user))
    total_time = time.time()-before
    print(f'Time to get timelines = {total_time} seconds')
    print(f'-> {n/total_time} api calls/second')
    print('----------------------')
    api.quit()

if __name__ == '__main__':
    # method 1
    print('Strategy 1:')
    run_tweet(RedisTwitterAPI_1)
    
    # method 2
    print('Strategy 2:')
    run_tweet(RedisTwitterAPI_2)