from api.twitter.twitterApi import *
import pytest

def test_get_user_info():
    user_id = "2892601"
    users_info = get_user_info( user_id)
    user_info = users_info[0][user_id]
    """ Check if returned userInfo is a list """
    assert type(users_info) is list
    """ Check if a single users info dict has 10 key/value pairs """
    assert len(user_info) is 10
    """ Check if all keys have defined values """
    for key, value in user_info.items():
        if not value:
            raise TypeError("Key %s for user %s is not defined!" % (key, user_id))

def test_get_tweet_info():
    tweet_id = "938472954735755264"
    tweets_info = get_tweet_info( tweet_id)
    tweet_info = tweets_info[0][tweet_id]
    """ Check if returned userInfo is a list """
    assert type(tweets_info) is list
    """ Check if a single tweet info dict has 8 key/value pairs """
    assert len(tweet_info) is 8
    """ Check if all keys have defined values """
    for key, value in tweet_info.items():
        if not value:
            raise TypeError("Key %s for tweet %s is not defined!" % (key, tweet_id))

def test_get_retweeters():
    tweet_id = "935572279693516800"
    followers_list = get_retweeters(tweet_id)
    """ Check if retweeters are returned as a list"""
    assert type(followers_list) is list
    """ Check if list contains just numeric & round strings """
    for uid in followers_list:
        try: 
            int(uid)
        except ValueError:
            pytest.fail("Retweeter user id %s is not convertible to long." % uid)