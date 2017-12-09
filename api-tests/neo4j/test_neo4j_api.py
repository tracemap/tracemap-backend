import api.neo4j.neo4jApi as neo4j
import api.twitter.twitterApi as twitter
import pytest

def test_get_followers():
    """Get an example tweet"""
    tweet_id = "834444544108281857"
    """Get all users that retweeted it as a list of uid strings"""
    retweeters = twitter.get_retweeters(tweet_id)
    """Guarantee that the returned value of the function is a list..."""
    assert type(retweeters) is list
    for retweeter in retweeters:
        """...whose elements are strings"""
        assert isinstance(retweeter, str) 
        """and that each string is, indeed, a positive number"""
        assert retweeter.isdigit()
    """Use the function get_followers to return a response dictionary"""
    followers_dictionary = neo4j.get_followers(retweeters)
    """First, guarantee that it is a dictionary"""
    assert type(followers_dictionary) is dict
    for user in followers_dictionary.keys():
        """Guarantee that the keys correspond to elements in the retweeters"""
        assert user in retweeters
        """Check that the format of the dictionary is correct"""
        assert "followers" in followers_dictionary[user].keys()
        """that under the 'followers' key one can access a list..."""
        assert type(followers_dictionary[user]["followers"]) is list
        for follower in followers_dictionary[user]["followers"]:
            """whose elements are also inside the retweeters list"""
            assert follower in retweeters

