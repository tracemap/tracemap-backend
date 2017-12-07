from flask import Flask, jsonify
from flask_cors import CORS

import api.twitter.twitterApi as twitterApi

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/twitter/get_retweeters/<string:tweet_id>')
def twitter_get_retweeters(tweet_id):
    return jsonify(twitterApi.get_retweeters(tweet_id))

@app.route('/twitter/get_tweet_info/<string:tweet_id>')
def twitter_get_tweet_info(tweet_id):
    return jsonify(twitterApi.get_tweet_info(tweet_id))

@app.route('/twitter/get_user_info/<string:user_id>')
def twitter_get_user_info(user_id):
    return jsonify(twitterApi.get_user_info(user_id))


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
