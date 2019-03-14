from typing import List
from users import User

class Tweet(object):
    """
    A Tweet representing the initial state of each tracemap.  
    :attr id: the tweets id (str)  
    :attr last_updated: unix timestamp representing last update of 
    the tweets data through twitter (str)  
    :attr author: author of the tweet  
    :attr retweeters: retweeters of the tweet  
    """
    def __init__(
        self,
        id: str,
        last_updated: str,
        author: User,
        retweeters: List[User]
    ):
        self.id = id
        self.last_updated = last_updated
        self.author = author
        self.retweeters = retweeters

class TraceMap(object):
    """
    A TraceMap representing a tweet with a list of paths 
    the tweet took while propagating.  
    :attr tweet: the corresponding tweet object  
    :attr paths: list of path dicts {source: uid, target: uid}
    """
    def __init__(self, tweet:Tweet, paths: List[dict]):
        self.tweet = tweet
        self.paths = paths
