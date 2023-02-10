"""
Twitter Redis API
"""

import redis
from tweet_object import Tweet

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
        # push to a followers of the user | push to follows of the user
        # index = followers:(user id) | follows:(user_id)
        # value = list of ids that follow the above user id | list of ids that the user follows
        for _, row in df.iterrows():
            self.r.lpush(f"followers:{int(row[1])}", int(row[0]))
            self.r.lpush(f"follows:{int(row[0])}", int(row[1]))
        
    def post_tweet(self, idx, twt):
        # post tweet and push tweet key value to user ids
        # auto incrementing from dataframe iterrows index
        # push value as hash
        self.r.hset(f"tweet:{idx}", mapping={'user': twt.user, 'ts': twt.ts, 'text':twt.text})
        
    def get_users(self):
        # get all keys and return list of integers
        all_keys = self.r.keys('tweet:*')
        
        # push tweet keys to users
        for key in all_keys:
            self.r.lpush(f"user:{self.r.hget(key, 'user')}", key)
                
        # set of user ids
        all_keys = self.r.keys('user:*')
        for key in all_keys:
            self.r.sadd('all_users', key.split(":")[1])
            
        return list(self.r.smembers('all_users'))
    
    def rand_user(self):
        # random user
        return self.r.srandmember('all_users')
        
    def get_user_timeline(self, user):
        # get ids of user follows
        for id in self.r.lrange(f'follows:{user}', 0, -1):
            # get tweets of user
            for twt in self.r.lrange(f'user:{id}', 0, -1):
                # sorted set of tweets based off their datetime
                self.r.zadd(f'timeline:{user}', {twt: float(self.r.hget(twt, 'ts'))})
            
    
        # get the 10 most recent posts for a users timeline
        return [Tweet(self.r.hget(twt, 'user'), self.r.hget(twt, 'ts'), self.r.hget(twt, 'text')) for twt in self.r.zrange(f'timeline{user}', 0, 9)]
    


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
        
    def post_tweet(self, idx, twt):
        # push to user_id
        twt_id = f"tweet:{idx}"
        self.r.hset(f"tweet:{idx}", mapping={'user': twt.user, 'ts': twt.ts, 'text':twt.text})
        
        # push to timeline of people who follow the above user
        for id in self.r.lrange(f'followers:{twt.user}', 0, -1):
            self.r.lpush(f"timeline:{id}", twt_id)
        
    def get_users(self):
        # get all keys and return list of integers
        all_keys = self.r.keys('followers:*')
        for key in all_keys:
            self.r.sadd('all_users', key.split(":")[1])
        
        # get unique list of integers from key values
        return list(self.r.smembers('all_users'))
    
    def rand_user(self):
        # get random user
        return self.r.srandmember('all_users')
        
    def get_user_timeline(self, user):
        # get the 10 most recent posts for a users timeline
        timeline = []
        for twt in self.r.lrange(f"timeline:{user}", 0, 9):
            twt_map = self.r.hgetall(twt)
            timeline.append(Tweet(twt_map['user'], twt_map['ts'], twt_map['text'])) 
            
        return timeline