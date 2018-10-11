from flask import Flask, jsonify, request
from flask_cors import CORS

import api.twitter.twitterApi as twitterApi
import api.twitter.tweet as tweet
import api.neo4j.neo4jApi as neo4jApi
import api.newsletter.newsletterApi as newsletterApi
import api.auth.betaAuth as betaAuth

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

"""Takes a single tweet_id string and returns a list of retweeter id strings"""
@app.route('/twitter/get_retweeters/<string:tweet_id>')
def twitter_get_retweeters(tweet_id):
    return jsonify(twitterApi.get_retweeters(tweet_id))

"""Takes a single tweet_id string and returns a tweet_info json object"""
@app.route('/twitter/get_tweet_info/<string:tweet_id>')
def twitter_get_tweet_info(tweet_id):
    return jsonify(twitterApi.get_tweet_info(tweet_id))

@app.route('/twitter/get_tweet_data/<string:tweet_id>')
def twitter_get_tweet_data(tweet_id):
    return jsonify(twitterApi.get_tweet_data(tweet_id))

@app.route('/twitter/get_user_timeline/<string:user_id>')
def twitter_get_user_timeline(user_id):
    return jsonify(twitterApi.get_user_timeline(user_id))

"""Takes a comma seperated list of user_ids
    returns a user_info json object
"""
@app.route('/twitter/get_user_info/<string:user_ids>')
def twitter_get_user_info(user_ids):
    return jsonify(twitterApi.get_user_info(user_ids.split(",")))

@app.route('/tweet/get_html/<string:tweet_id>')
def tweet_get_html(tweet_id):
    return jsonify(tweet.get_html(tweet_id))

"""Takes a comma seperated list of user_ids and returns the subnetwork of followship
   relations between those users
"""
@app.route('/neo4j/get_followers/<string:user_ids>')
def neo4j_get_followers(user_ids):
    return jsonify(neo4jApi.get_followers(user_ids.split(",")))

@app.route('/neo4j/get_user_info/<string:user_id>')
def neo4j_get_user_info(user_id):
    return jsonify(neo4jApi.get_user_info(user_id))

@app.route('/neo4j/label_unknown_users/<string:user_ids>')
def neo4j_label_unknown_users(user_ids):
    return jsonify(neo4jApi.label_unknown_users(user_ids.split(",")))

@app.route('/newsletter/save_subscriber', methods= ['POST'])
def newsletter_save_subscriber():
    body = request.get_json()
    email = body['email']
    return jsonify(newsletterApi.save_subscriber(email))

@app.route('/auth/check_password', methods = ['POST'])
def auth_check_password():
    body = request.get_json()
    email = body['email']
    password = body['password']
    return jsonify(betaAuth.check_password(email, password))

@app.route('/auth/add_user', methods = ['POST'])
def auth_add_user():
    body = request.get_json()
    email = body['email']
    username = body['username']
    return jsonify(betaAuth.add_user(username, email))

@app.route('/auth/delete_user', methods = ['POST'])
def auth_delete_user():
    body = request.get_json()
    email = body['email']
    password = body['password']
    return jsonify(betaAuth.delete_user(email, password))

@app.route('/auth/change_password', methods = ['POST'])
def auth_change_password():
    body = request.get_json()
    email = body['email']
    old_password = body['old_password']
    new_password = body['new_password']
    return jsonify(betaAuth.change_password(email, old_password, new_password))

@app.route('/auth/get_user_data', methods = ['POST'])
def auth_get_user_data():
    body = request.get_json()
    email = body['email']
    session_token = body['session_token']
    return jsonify(betaAuth.get_user_data(email, session_token))

@app.route('/auth/check_session', methods = ['POST'])
def auth_check_session():
    body = request.get_json()
    email = body['email']
    session_token = body['session_token']
    return jsonify(betaAuth.check_session(email, session_token))

@app.route('/auth/reset_password', methods = ['POST'])
def auth_reset_password():
    body = request.get_json()
    email = body['email']
    reset_token = body['reset_token']
    return betaAuth.reset_password(email, reset_token)

@app.route('/auth/request_reset_password', methods = ['POST'])
def auth_request_reset_password():
    body = request.get_json()
    email = body['email']
    return jsonify(betaAuth.request_reset_user(email))

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
