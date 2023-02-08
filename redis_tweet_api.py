"""
Twitter Redis API
"""

import redis

class RedisTwitterAPI_1:

    def __init__(self, host="localhost", port=6379, decode=True):
        # authenticate
        self.r = redis.Redis(host=host, port=port, decode_responses=decode)
        self.r.flushall()
        
    def quit(self):
        # close connection
        self.r.quit()
        self.r = None
        
    def generate_follows_from_dataframe(self, df):
        # push to a followers of the user
        # index = followers:(user id)
        # value = list of ids that follow the above user id
        for _, row in df.iterrows():
            self.r.lpush(f"followers:{int(row[1])}", int(row[0]))
        
    def post_tweet(self, twt):
        # post tweet and push tweet key value to user ids
        # auto incrementing
        self.r.incr('next')
        idx = self.r.get('next')
        
        tweet = f"tweet:{idx}"
        self.r.set(tweet, twt.text)
        self.r.lpush(f"user:{twt.user}", tweet)
        
    def get_users(self):
        # get all keys and return list of integers
        all_keys = self.r.keys()
        all_keys = [key for key in all_keys if 'user' in key]
        
        # get unique list of integers from key values
        ids = list(set([int(''.join(filter(str.isdigit, key))) for key in all_keys]))
        return ids
        
    def get_user_timeline(self, user):
        # get ids of followers
        for id in self.r.lrange(f'followers:{user}', 0, -1):
            # get tweets of user
            for twt in self.r.lrange(f'user:{id}', 0, -1):
                # sorted set of tweets based off their datetime
                self.r.zadd(f'timeline:{user}', {self.r.get(twt): int(twt.split(':')[1])})
            
    
        # get the 10 most recent posts for a users timeline
        timeline = self.r.zrange(f"timeline:{user}", 0, 9)
        return timeline
    


class RedisTwitterAPI_2:

    def __init__(self, host="localhost", port=6379, decode=True):
        # authenticate
        self.r = redis.Redis(host=host, port=port, decode_responses=decode)
        self.r.flushall()
        
    def quit(self):
        # close connection
        self.r.quit()
        self.r = None
        
    def generate_follows_from_dataframe(self, df):
        # push to a followers of the user
        # index = followers:(user id)
        # value = list of ids that follow the above user id
        for _, row in df.iterrows():
            self.r.lpush(f"followers:{int(row[1])}", int(row[0]))
        
    def post_tweet(self, twt):
        # push to user_id
        tweet=f"tweet{twt.ts}"
        self.r.set(tweet, twt.text)
        self.r.lpush(f"user:{twt.user}", tweet)
        
        # push to timeline of people who follow the above user
        for id in self.r.lrange(f'followers:{twt.user}', 0, -1):
            self.r.lpush(f"timeline:{id}", twt.text)
        
    def get_users(self):
        # get all keys and return list of integers
        all_keys = self.r.keys()
        all_keys = [key for key in all_keys if 'user' in key]
        
        # get unique list of integers from key values
        ids = list(set([int(''.join(filter(str.isdigit, key))) for key in all_keys]))
        return ids
        
    def get_user_timeline(self, user):
        # get the 10 most recent posts for a users timeline
        timeline = self.r.lrange(f"timeline:{user}", 0, 9)
        return timeline