from flask import Flask, jsonify

import api.twitter.twitterApi as twitterApi

app = Flask(__name__)

@app.route('/twitter/get_retweeters/<string:tweet_id>')
def twitter_get_retweeters(tweet_id):
    return jsonify(twitterApi.get_retweeters(tweet_id))

@app.route('/twitter/get_user_info/<string:user_id>')
def twitter_get_user_info(user_id):
    return jsonify(twitterApi.get_user_info(user_id))

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
