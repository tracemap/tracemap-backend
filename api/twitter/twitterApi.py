import json
import time

from twitter import *
from TwitterAPI import TwitterAPI
from credentials import *


credIndex = 0

api = TwitterAPI(APPLICATION['token'], APPLICATION['secret'], USERS[credIndex]['token'], USERS[credIndex]['secret'])

# Switch to the next user when running out of requests 
def __changeCredentials():
    """Switch to the next user credentials if running out of requests"""
    credIndex = (credIndex + 1) % len(USERS)   
    twit = Twitter( format="json",
        auth=OAuth(USERS[credIndex]['token'], USERS[credIndex]['secret'], APPLICATION['token'], APPLICATION['secret'] ))

def __formatUserInfo( data):
    """Format getUserInfo data as a dictionary of relevant data"""
    response = {}
    response[ data['id_str']] = {}
    userDict = response[ data['id_str']]
    userDict[ "timestamp"] = str(time.time())
    userDict[ "name"] = str( data['name'])
    userDict[ "screen_name"] = str( data['screen_name'])
    userDict[ "location"] = str( data['location'])
    userDict[ "lang"] = str(data['lang'])
    userDict[ "followers_count"] = int(data['followers_count'])
    userDict[ "friends_count"] = int(data['friends_count'])
    userDict[ "statuses_count"] = int(data['statuses_count'])
    userDict[ "created_at"] = str(data['created_at'])
    userDict[ "profile_img_url"] = str(data['profile_image_url'])
    return( response)


def getUserInfo( uid):
    """Request Useer Information, return a dictionary"""
    if type(uid) is str:
        data = api.request('users/show', {'user_id': uid})
        return [__formatUserInfo(data.json())]
    if type(uid) is list:
        results = []
        for id in uid:
            data = api.request('users/show',  {'user_id': id})
            results.append( __formatUserInfo( data.json()))
        return results
    else:
        print("Invalid data, need string or list at getUserInfo")
        return ""

def getRetweeters( tweetId):
    """Request the 100 last retweet ids, return them as a list"""
    data = api.request('statuses/retweeters/ids', { 'id': str(tweetId)})
    return data.json()['ids']


result = getUserInfo("1953985920")

print( json.dumps(result, indent=2))