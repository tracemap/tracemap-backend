from flask import Flask, jsonify, request, Response
from flask_cors import CORS

from elasticapm.contrib.flask import ElasticAPM

import api.neo4j.neo4jApi as neo4jApi
import api.newsletter.newsletterApi as newsletterApi
import api.auth.betaAuth as betaAuth
import api.logging.logger as logger

from api.twitter.twitterApi import TracemapTwitterApi
twitterApi = TracemapTwitterApi()

app = Flask(__name__)
apm = ElasticAPM(app, logging=True)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

def __is_session_valid(email: str, session_token: str):
    if email and session_token:
        response = betaAuth.check_session(email, session_token)
        return response.get('session', False)
    else:
        return False

@app.route('/status')
def health_check():
    """Request health status of the api"""
    return Response("OK", status=200)

@app.route('/twitter/get_tweet_info', methods = ['POST'])
def twitter_get_tweet_info():
    """
    Returns shortform of twitter_get_tweet_data
    to get e.g. the number of retweets for a tweet
    """
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("session_token","email", "tweet_id")):
        session_token = body['session_token']
        email = body['email']
        tweet_id = body['tweet_id']
        if __is_session_valid(email, session_token):
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
    ("session_token","email", "tweet_id")):
        session_token = body['session_token']
        email = body['email']
        tweet_id = body['tweet_id']
        if __is_session_valid(email, session_token):
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
    ("session_token","email", "user_id")):
        session_token = body['session_token']
        email = body['email']
        user_id = body['user_id']
        if __is_session_valid(email, session_token):
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
    ("session_token","email", "user_ids")):
        session_token = body['session_token']
        email = body['email']
        user_ids = body['user_ids']
        if __is_session_valid(email, session_token):
            return jsonify(twitterApi.get_user_info(user_ids))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)

@app.route('/neo4j/get_followers', methods = ['POST'])
def neo4j_get_followers():
    """
    Takes a comma seperated list of user_ids and returns the subnetwork of followship
    relations between those users
    """
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("session_token","email", "user_ids")):
        session_token = body['session_token']
        email = body['email']
        user_ids = body['user_ids']
        if __is_session_valid(email, session_token):
            return jsonify(neo4jApi.get_followers(user_ids))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)

@app.route('/neo4j/label_unknown_users', methods = ['POST'])
def neo4j_label_unknown_users():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("session_token","email", "user_ids")):
        session_token = body['session_token']
        email = body['email']
        user_ids = body['user_ids']
        if __is_session_valid(email, session_token):
            return jsonify(neo4jApi.label_unknown_users(user_ids))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)

@app.route('/newsletter/save_subscriber', methods= ['POST'])
def newsletter_save_subscriber():
    body = request.get_json()
    if body and "email" in body:
        email = body['email']
        return jsonify(newsletterApi.save_subscriber(email))
    else:
        return Response("Bad Request", status=400)

@app.route('/auth/check_password', methods = ['POST'])
def auth_check_password():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("password","email")):
        email = body['email']
        password = body['password']
        return jsonify(betaAuth.check_password(email, password))
    else:
        return Response("Bad Request", status=400)

@app.route('/auth/add_user', methods = ['POST'])
def auth_add_user():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("username","email")):
        email = body['email']
        username = body['username']
        return jsonify(betaAuth.add_user(username, email))
    else:
        return Response("Bad Request", status=400)

@app.route('/auth/delete_user', methods = ['POST'])
def auth_delete_user():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("password","email")):
        email = body['email']
        password = body['password']
        return jsonify(betaAuth.delete_user(email, password))
    else:
        return Response("Bad Request", status=400)

@app.route('/auth/change_password', methods = ['POST'])
def auth_change_password():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("email", "old_password", "new_password")):
        email = body['email']
        old_password = body['old_password']
        new_password = body['new_password']
        return jsonify(betaAuth.change_password(email, old_password, new_password))
    else:
        return Response("Bad Request", status=400)

@app.route('/auth/get_user_data', methods = ['POST'])
def auth_get_user_data():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("email", "session_token")):
        email = body['email']
        session_token = body['session_token']
        return jsonify(betaAuth.get_user_data(email, session_token))
    else:
        return Response("Bad Request", status=400)

@app.route('/auth/check_session', methods = ['POST'])
def auth_check_session():
    body = request.get_json()
    if body and all (keys in body for keys in 
    ("session_token","email")):
        email = body['email']
        session_token = body['session_token']
        return jsonify(betaAuth.check_session(email, session_token))
    else:
        return Response("Bad Request", status=400)

@app.route('/auth/reset_password/<string:email>/<string:reset_token>')
def auth_reset_password(email: str, reset_token: str):
    return betaAuth.reset_password(email, reset_token)

@app.route('/auth/request_reset_password', methods = ['POST'])
def auth_request_reset_password():
    body = request.get_json()
    if body and "email" in body:
        email = body['email']
        return jsonify(betaAuth.request_reset_user(email))
    else:
        return Response("Bad Request", status=400)
    
@app.route('/logging/write_log', methods = ['POST'])
def logging_write_log():
    required_parameters = (
        "email",
        "session_token",
        "file_name",
        "log_object")
    body = request.get_json()
    if body and all (key in body for key in required_parameters):
        email = body["email"]
        session_token = body["session_token"]
        if __is_session_valid(email, session_token):
            log_object = body["log_object"]
            file_name = body["file_name"]
            log_object['email'] = email
            return jsonify(logger.save_log(log_object, file_name))
        else:
            return Response("Forbidden", status=403)
    else:
        return Response("Bad Request", status=400)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
