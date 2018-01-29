import json
import time
import os

from . import tweet as tweetAPI

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

def __parse_properties( data, keys):
    response = {}
    for key in keys:
        response[key] = data[key]
    return response


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

def __format_tweet_data( data):
    response = {}
    response['retweeter_ids'] = []
    response['retweet_info'] = {}
    tweet_info_keys = [
        'id_str',
        'created_at',
        'lang',
        'favorite_count',
        'retweet_count',
        'entities',
        'source',
        'text',
        'is_quote_status',
        'in_reply_to_status_id_str',
        'in_reply_to_user_id_str'
    ]
    user_info_keys = [
        'id_str',
        'created_at',
        'name',
        'screen_name',
        'description',
        'favourites_count',
        'followers_count',
        'friends_count',
        'profile_image_url_https',
        'statuses_count',
        'verified',
        'location',
        'lang'
    ]
    tmp = data[0]['retweeted_status']
    response['tweet_info'] = __parse_properties(tmp, tweet_info_keys)
    tmp = tmp['user']
    response['tweet_info']['user'] = __parse_properties(tmp, user_info_keys)

    for retweet in data:
        retweet_dict = __parse_properties(retweet, tweet_info_keys)
        tmp = retweet['user']
        retweet_dict['user'] = __parse_properties(tmp, user_info_keys)
        response['retweeter_ids'].append(tmp['id_str'])
        response['retweet_info'][tmp['id_str']] = retweet_dict
    return response

def __request_user_timeline( user_id, include_rts = True):
    params = {
        'user_id': user_id, 
        'exclude_replies': False,
        'count': 200
    }
    if not include_rts:
        params['include_rts'] = False
    url = "statuses/user_timeline";
    data = api.request(url, params)
    return data.json()

        

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

def get_tweet_data( tweet_id):
    """Request full tweet information, including retweet and user information"""
    url = "statuses/retweets/:%s" % tweet_id
    data = api.request(url, {'count': 100}).json()
    results = {}
    results['response'] = __format_tweet_data(data)
    return results

def get_user_timeline( user_id):
    """Get the latest tweets of a user.
       Returns up to 200 retweets in 4 categories."""

    tweet_object = {}
    tmp_tweets = __request_user_timeline( user_id)
    response = {}
    response['by_time'] = []
    for tweet in tmp_tweets:
        tmp_response = {}
        tmp_response['id_str'] = tweet['id_str']
        tmp_response['retweet_count'] = tweet['retweet_count']
        tmp_response['retweeted'] = True
        tmp_response['created_at'] = tweet['created_at']
        if 'retweeted_status' not in tweet:
            tmp_response['retweeted'] = False
        response['by_time'].append(tmp_response)

    response['by_retweets'] = sorted(response['by_time'], key=lambda tweet: tweet['retweet_count'], reverse=True)
    response['by_time_no_rts'] = list(filter(
        lambda x: not x['retweeted'], 
        response['by_time']))
    response['by_retweets_no_rts'] = list(filter(
        lambda x: not x['retweeted'], 
        response['by_retweets']))
    return response

