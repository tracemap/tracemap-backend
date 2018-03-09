from flask import Flask, jsonify
from flask_cors import CORS

import api.twitter.twitterApi as twitterApi
import api.twitter.tweet as tweet
import api.neo4j.neo4jApi as neo4jApi

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
    return jsonify(twitterApi.get_user_timeline(user_id));

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

@app.route('/neo4j/crawl_unknown_users/<string:user_ids>')
def neo4j_crawl_unknown_followers(user_ids):
    return neo4jApi.crawl_unknown_users(user_ids.split(","))

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
