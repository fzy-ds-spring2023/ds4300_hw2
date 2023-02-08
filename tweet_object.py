'''
    Define the Tweet class object here
'''

class Tweet:

    def __init__(self, user, ts, text):
        self.user = user
        self.ts = ts
        self.text = text
        
    def __str__(self):
        return f"User {self.user} at {self.ts}:\n{self.text}"
