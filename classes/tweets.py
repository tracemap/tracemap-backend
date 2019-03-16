from typing import List
from classes.users import User

class Tweet(object):
    """
    A Tweet representing the initial state of each tracemap.  
    :attr data: id(str), last_updated(time-str), 
    author(User), retweeters(User[])
    """
    def __init__(self, data: dict):
        self.id = data['id']
        self.last_updated = data['last_updated']
        self.author = data['author']
        self.retweeters = data['retweeters']

class TraceMap(Tweet):
    """
    A TraceMap representing a tweet with a list of paths 
    the tweet took while propagating.  
    :attr data: Tweets data & paths ({'source': '123', 'target':'234'}) 
    """
    def __init__(self, data: dict):
        super().__init__(data)
        self.paths = data['paths']
