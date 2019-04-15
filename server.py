from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import json

from elasticapm.contrib.flask import ElasticAPM

from api.neo4j.neo4jApi import Neo4jApi
import api.logging.logger as logger

import api.twitter.twitterAuth as twitterAuth
from api.twitter.twitterApi import TracemapTwitterApi
from api.neo4j.tracemapUserAdapter import TracemapUserAdapter
import api.user.newsletterModule as newsletterModule

from classes.users import TmUser, User

userAdapter = TracemapUserAdapter()
neo4jApi = Neo4jApi()
app = Flask(__name__)
# apm = ElasticAPM(app, logging=True)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

def __get_tracemap_twitter_api(user_id: str, session_token: str) -> TracemapTwitterApi:
    tm_user = neo4jApi.get_tm_user(user_id)
    if tm_user.session_token == session_token:
        return TracemapTwitterApi(tm_user)
    else:
        return None

@app.route('/status')
def health_check():
    """Request health status of the api"""
    return Response("OK", status=200)

@app.route('/twitter/start_authenticate', methods = ['GET'])
def twitter_start_authenticate():
    return jsonify(twitterAuth.start_authentication())

@app.route('/twitter/complete_authenticate', methods = ['POST'])
def twitter_complete_authenticate():
    """
    Returns the users username on successful authentication.
    """
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("oauth_token", "oauth_verifier")):
        oauth_token = body['oauth_token']
        oauth_verifier = body['oauth_verifier']
        return jsonify(twitterAuth.complete_authentication(oauth_token, oauth_verifier))
    else:
        return Response("Bad Request", status=400)

@app.route('/twitter/check_session', methods = ['POST'])
def twitter_check_session():
    """
    Compares a session_token with the database users session_tokens 
    and returns the users credential if token matches
    """
    body = request.get_json()
    if body and all (keys in body for keys in
    ("auth_user_id", "auth_session_token")):
        auth_user_id = body['auth_user_id']
        auth_session_token = body['auth_session_token']
        return jsonify(twitterAuth.check_session_token(auth_user_id, auth_session_token))
    else:
        return Response("Bad Request", status=400)
    

@app.route('/add_me')
def add_me():
    user_dict = {}
    user_dict['auth_token'] = '795585054831443968-oUFJaTgfLKyQ1mOVaskWGSQyv6BQOJk' 
    user_dict['auth_secret'] = 'pUUuZfIHNG8NaEVZu0Xe1HhBgMLRn2JpSgAsutuQxIlkU'
    user_dict['id'] = '795585054831443968'
    user_dict['session_token'] = 'not-set'
    tm_user = TmUser(user_dict)
    return make_json(neo4jApi.write_tm_user(tm_user))


@app.route('/test', methods=['GET'])
def test_something():
    tweet_id = '847112'
    return make_json(neo4jApi.get_tweet_data(tweet_id))

def make_json(in_data) -> str:
    return json.dumps(in_data, default=lambda o: o.__dict__ if hasattr(o, '__dict__') else o)

@app.route('/twitter/get_tweet_info', methods = ['POST'])
def twitter_get_tweet_info():
    """
    Returns shortform of twitter_get_tweet_data
    to get e.g. the number of retweets for a tweet
    """
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("auth_session_token","auth_user_id", "tweet_id")):
        auth_session_token = body['auth_session_token']
        auth_user_id = body['auth_user_id']
        tweet_id = body['tweet_id']
        twitterApi = __get_tracemap_twitter_api(auth_user_id, auth_session_token)
        if twitterApi:
            return jsonify(twitterApi.get_tweet_info(tweet_id))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)

@app.route('/twitter/get_tweet_data', methods = ['POST'])
def twitter_get_tweet_data():
    """
    Returns data of a tweet to get the detailed
    tweet data (retweeter_ids etc.)
    """
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("auth_session_token","auth_user_id", "tweet_id")):
        auth_session_token = body['auth_session_token']
        auth_user_id = body['auth_user_id']
        tweet_id = body['tweet_id']
        twitterApi = __get_tracemap_twitter_api(auth_user_id, auth_session_token)
        if twitterApi:
            return jsonify(twitterApi.get_tweet_data(tweet_id))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)
        

@app.route('/twitter/get_user_timeline', methods = ['POST'])
def twitter_get_user_timeline():
    """
    Takes a user_id and returns the last 200
    tweets/retweets of this user
    """
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("auth_session_token","auth_user_id", "user_id")):
        auth_session_token = body['auth_session_token']
        auth_user_id = body['auth_user_id']
        user_id = body['user_id']
        twitterApi = __get_tracemap_twitter_api(auth_user_id, auth_session_token)
        if twitterApi:
            return jsonify(twitterApi.get_user_timeline(user_id))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)

@app.route('/twitter/get_user_info', methods = ['POST'])
def twitter_get_user_info():
    """
    Takes a comma seperated list of user_ids
    returns a user_info json object
    """
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("auth_session_token","auth_user_id", "user_ids")):
        auth_session_token = body['auth_session_token']
        auth_user_id = body['auth_user_id'] 
        user_ids = body['user_ids']
        twitterApi = __get_tracemap_twitter_api(auth_user_id, auth_session_token)
        if twitterApi:
            return jsonify(twitterApi.get_user_infos(user_ids))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)

# @app.route('/neo4j/get_followers', methods = ['POST'])
# def neo4j_get_followers():
#     """
#     Takes a comma seperated list of user_ids and returns the subnetwork of followship
#     relations between those users
#     """
#     body = request.get_json()
#     if body and all (keys in body for keys in 
#     ("session_token","email", "user_ids")):
#         session_token = body['auth_session_token']
#         email = body['email']
#         user_ids = body['user_ids']
#         if __is_session_valid(email, session_token):
#             return jsonify(neo4jApi.get_followers(user_ids))
#         else:
#             return Response("Forbidden", status=403)
#     else:
#         return Response("Bad Request", status=400)

# @app.route('/neo4j/label_unknown_users', methods = ['POST'])
# def neo4j_label_unknown_users():
#     body = request.get_json()
#     if body and all (keys in body for keys in 
#     ("session_token","email", "user_ids")):
#         session_token = body['auth_session_token']
#         email = body['email']
#         user_ids = body['user_ids']
#         if __is_session_valid(email, session_token):
#             return jsonify(neo4jApi.label_unknown_users(user_ids))
#         else:
#             return Response("Forbidden", status=403)
#     else:
#         return Response("Bad Request", status=400)

@app.route('/newsletter/start_subscription', methods = ['POST'])
def newsletter_start_subscription():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("email", "newsletter_subscribed", "beta_subscribed")):
        email = body['email']
        newsletter_subscribed = body['newsletter_subscribed']
        beta_subscribed = body['beta_subscribed']
        return jsonify(newsletterModule.start_save_subscriber(email, newsletter_subscribed, beta_subscribed))
    else:
        return Response("Bad Request", status=400)

@app.route('/newsletter/confirm_subscription/<string:email>/<string:confirmation_token>')
def newsletter_confirm_subscription(email:str, confirmation_token: str):
    return newsletterModule.save_subscriber(email, confirmation_token)

# @app.route('/logging/write_log', methods = ['POST'])
# def logging_write_log():
#     required_parameters = (
#         "email",
#         "session_token",
#         "file_name",
#         "log_object")
#     body = request.get_json()
#     if body and all (key in body for key in required_parameters):
#         email = body["email"]
#         session_token = body["session_token"]
#         if __is_session_valid(email, session_token):
#             log_object = body["log_object"]
#             file_name = body["file_name"]
#             log_object['email'] = email
#             return jsonify(logger.save_log(log_object, file_name))
#         else:
#             return Response("Forbidden", status=403)
#     else:
#         return Response("Bad Request", status=400)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
