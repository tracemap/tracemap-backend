import api.neo4j.neo4jApi as neo4j
import api.twitter.twitterApi as twitter
import pytest

def test_get_followers():
    """Get an example tweet"""
    tweet_id = "834444544108281857"
    """Get all users that retweeted it as a list of uid strings"""
    retweeters = twitter.get_retweeters(tweet_id)
    """Guarantee that the returned value of the function is a list..."""
    assert type(retweeters) is dict
    for retweeter in retweeters['response']:
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

def test_add_user_info():
    """Example of users already in the database for the test to work!!!"""
    example_users = ['8557', '56', '560306494', '625025485']
    """Use this function to get public info from Twitter"""
    users_info = twitter.get_user_info(example_users)
    """Guarantee that the data is in a dictionary..."""
    assert type(users_info) is dict
    """...with a 'response' key."""
    assert 'response' in users_info
    for uid in users_info['response'].keys():
        """Check if each user id contains a dictionary"""
        assert type(users_info['response'][uid]) is dict

    """Call the function add_user_info() to write to the database"""
    success = neo4j.add_user_info(users_info)
    """This test does not check if the info was written in the database."""
    assert success
    
def test_get_user_info():
    """Example of a user already in the database"""
    user_id = "8557"
    user_info = neo4j.get_user_info(user_id)
    properties = ["timestamp","name","screen_name","location","lang",
                  "followers_count","friends_count","statuses_count",
                  "created_at","profile_img_url","uid"]
    """Guarantee that the info is a dictionary"""
    assert type(user_info) is dict
    """Guarantee that 'response' is in the dictionary"""
    assert 'response'in user_info
    for property_name in user_info['response'][user_id]:
        assert property_name in properties
