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
    user_dict = {}
    user_dict[ "timestamp"] = str(time.time())
    user_dict[ "name"] = str( data['name'])
    user_dict[ "screen_name"] = str( data['screen_name'])
    user_dict[ "location"] = str( data['location'])
    user_dict[ "lang"] = str(data['lang'])
    user_dict[ "followers_count"] = int(data['followers_count'])
    user_dict[ "friends_count"] = int(data['friends_count'])
    user_dict[ "statuses_count"] = int(data['statuses_count'])
    user_dict[ "created_at"] = str(data['created_at'])
    user_dict[ "profile_image_url"] = str(data['profile_image_url'])
    return( user_dict)

def __format_tweet_info( data):
    data = data[0]
    response = {}
    response['response'] = {}
    response['response'][ data['id_str']] = {}
    tweet_dict = response['response'][ data['id_str']]
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

def get_user_info( uid_list):
    """Request user information, return a dictionary"""
    results = {}
    results['response'] = {}
    for id in uid_list:
        data = api.request('users/show',  {'user_id': id})
        results['response'][ str(id)] = __format_user_info( data.json())
    return results

def get_tweet_info( tweet_id):
    """Request tweet information, return a dictionary"""
    data = api.request('statuses/lookup', {'id': tweet_id})
    return __format_tweet_info(data.json())

def get_retweeters( tweet_id):
    """Request the 100 last retweet ids, return them as a list"""
    data = api.request('statuses/retweeters/ids', { 'id': str(tweet_id)})
    response = {}
    response['response'] = data.json()['ids']
    retweeters = response['response']
    """change user_ids from num to string"""
    for index, num in enumerate(retweeters):
        retweeters[index] = str(num)
    return response

