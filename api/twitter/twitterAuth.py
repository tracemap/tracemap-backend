import requests
import json
import os
import string
import random
import oauth2
from urllib.parse import parse_qsl
from api.neo4j.neo4jApi import Neo4jApi
from classes.users import TmUser

neo4jApi = Neo4jApi()

consumer_key = os.environ.get('APP_TOKEN')
consumer_secret = os.environ.get('APP_SECRET')

request_token_url = 'https://api.twitter.com/oauth/request_token?oauth_callback=http://localhost:4200/home&x_auth_access_type=read'
access_token_url = 'https://api.twitter.com/oauth/access_token'
authorize_url = 'https://api.twitter.com/oauth/authorize'

consumer = oauth2.Consumer(consumer_key, consumer_secret)
client = oauth2.Client(consumer)

def __generate_random_token() -> str:
    """
    Generates a random password string with length 10  
    :returns: the password string
    """
    chars = string.ascii_lowercase + string.digits
    size = 30
    return ''.join(random.choice(chars) for x in range(size))

def start_authentication() -> dict:
    resp, content = client.request(request_token_url, "GET")
    if resp['status'] != '200':
        raise Exception("Invalid response %s." % resp['status'])
    response = dict(parse_qsl(content.decode("utf-8")))
    if response['oauth_callback_confirmed']:
        neo4j_response = neo4jApi.write_oauth_user(response)
        if neo4j_response:
            return {
                'success': True,
                'redirect_url': "https://api.twitter.com/oauth/authenticate?oauth_token=%s" % response['oauth_token']}
        else:
            return {'success': False, 'status': 'Neo4j error'}
    else:
        return {'success': False, 'status': 'unconfirmed callback url'}

def complete_authentication(oauth_token: str, oauth_verifier: str) -> dict:
    neo4j_response = neo4jApi.get_oauth_user_secret(oauth_token)
    oauth_token_secret = neo4j_response['oauth_token_secret']
    token = oauth2.Token(oauth_token, oauth_token_secret)
    token.set_verifier(oauth_verifier)
    client = oauth2.Client(consumer, token)
    resp, content = client.request(access_token_url, "POST")
    if resp['status'] != '200':
        raise Exception("Invalid response %s." % resp['status'])
    twitter_response = dict(parse_qsl(content.decode("utf-8")))
    twitter_response['session_token'] = __generate_random_token()
    print(twitter_response)
    tm_user = TmUser(twitter_response)
    neo4jApi.write_tm_user(tm_user)
    return {
        'session_token': twitter_response['session_token'],
        'success': True,
        'screen_name': twitter_response['screen_name'],
        'user_id': twitter_response['user_id'],
    }
    
def check_session_token(user_id: string, session_token: string) -> dict:
    db_user = neo4jApi.get_tm_user(user_id)
    if  db_user.session_token:
        if db_user.session_token == session_token:
            return {
                'success': True,
                'screen_name': db_user.screen_name
            }
        else:
            return {
                'success': False,
                'status': 'session_token invalid'
            }
    else:
        return {
            'success': False,
            'status': 'user has no session token (this should not happen).'
        }