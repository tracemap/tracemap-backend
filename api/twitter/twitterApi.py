import json
import time
import os

from TwitterAPI import TwitterAPI

cred_index = 0

api = TwitterAPI( os.environ.get('APP_TOKEN'), 
                  os.environ.get('APP_SECRET'), 
                  os.environ.get('USER_TOKEN'), 
                  os.environ.get('USER_SECRET'))

def __change_credentials():
    """Switch to the next user credentials if running out of requests"""
    cred_index = (credIndex + 1) % len(USERS)   
    api = TwitterAPI(APPLICATION['token'], 
                     APPLICATION['secret'], 
                     USERS[cred_index]['token'], 
                     USERS[cred_index]['secret'])

def __format_user_info( data):
    """Format get_user_info data as a dictionary of relevant data"""
    response = {}
    response[ data['id_str']] = {}
    user_dict = response[ data['id_str']]
    user_dict[ "timestamp"] = str(time.time())
    user_dict[ "name"] = str( data['name'])
    user_dict[ "screen_name"] = str( data['screen_name'])
    user_dict[ "location"] = str( data['location'])
    user_dict[ "lang"] = str(data['lang'])
    user_dict[ "followers_count"] = int(data['followers_count'])
    user_dict[ "friends_count"] = int(data['friends_count'])
    user_dict[ "statuses_count"] = int(data['statuses_count'])
    user_dict[ "created_at"] = str(data['created_at'])
    user_dict[ "profile_img_url"] = str(data['profile_image_url'])
    return( response)

def __format_tweet_info( data):
    data = data[0]
    response = {}
    response[ data['id_str']] = {}
    tweet_dict = response[ data['id_str']]
    tweet_dict["reply_to"] = str( data['in_reply_to_status_id_str'])
    tweet_dict["lang"] = str( data['lang'])
    tweet_dict["author"] = str( data['user']['id_str'])
    tweet_dict["fav_count"] = str( data['favorite_count'])
    tweet_dict["retweet_count"] = str( data['retweet_count'])
    tweet_dict["date"] = str( data['created_at'])
    """The following values are lists"""
    tweet_dict["hashtags"] = data['entities']['hashtags']
    tweet_dict["user_mentions"] = data['entities']['user_mentions']
    return( response)

def get_user_info( uid):
    """Request user information, return a dictionary"""
    if type(uid) is str:
        data = api.request('users/show', {'user_id': uid})
        return [__format_user_info(data.json())]
    if type(uid) is list:
        results = []
        for id in uid:
            data = api.request('users/show',  {'user_id': id})
            results.append( __format_user_info( data.json()))
        return results
    else:
        print("Invalid data, need string or list at get_user_info")
        return ""

def get_tweet_info( tweet_id):
    """Request tweet information, return a dictionary"""
    if type(tweet_id) is str:
        data = api.request('statuses/lookup', {'id': tweet_id})
        return [__format_tweet_info(data.json())]
    else:
        print("Invalid data, need string at get_tweet_info")
        return ""

def get_retweeters( tweet_id):
    """Request the 100 last retweet ids, return them as a list"""
    data = api.request('statuses/retweeters/ids', { 'id': str(tweet_id)})
    retweets = data.json()['ids']
    for index, num in enumerate(retweets):
        retweets[index] = str(num)
    return retweets

