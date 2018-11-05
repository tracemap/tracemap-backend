import json
import time
import os

from TwitterAPI import TwitterAPI
from tokenProvider import Token

class TTA:

    def __request_twitter(self, route, params):
        if not hasattr(self, route):
            token_instance = Token(route)
            setattr(self, route, token_instance)
        api = getattr(self, route).api
        while True:
            try:
                response = api.request(route, params)
            except Exception as exc:
                print("Error while requesting Twitter: %s" % exc)
                time.sleep(10)
                continue
            parsed_response = json.loads(response.text)
            if parsed_response == []:
                return parsed_response
            error_response = self.__check_error(api, parsed_response)
            if error_response:
                if error_response == 'continue':
                    continue
                else:
                    print('###Error from Twitter:%s\n###Route requested:%s\n###Params:%s' % (error_response, route, params))
            else:
                return parsed_response

    def __check_error(self, api, response):
        error_response = ""
        if 'error' in response:
            error_response = self.__check_twitter_error_code(
                response["error"])
        elif 'errors' in response:
            error_response = self.__check_twitter_error_code(
                response["errors"][0]["code"])
        if error_response == "":
            return False
        else:
            if error_response == "Switch helper":
                api.__get_user_auth()
                return "continue"
            elif error_response == "Invalid user":
                return "invalid user"
            elif error_response == "Not authorized":
                return "invalid user"
            else:
                self.__log_to_file("7 - Unusual error response: %s" % error_response)
                return error_response

    @staticmethod
    def __check_twitter_error_code(code):
        return {
            32: "Switch helper",
            50: "Invalid user",
            63: "Invalid user",
            "Not authorized.": "Not authorized",
            88: "Switch helper",
            89: "Switch helper",
            131: "Internal error"
        }.get(code, "Unknown error %s" % code)

    def request_user_timeline(self, user_id, include_rts = True):
        params = {
            'user_id': user_id, 
            'exclude_replies': False,
            'count': 200
        }
        if not include_rts:
            params['include_rts'] = False
        route = "statuses/user_timeline"
        data = self.__request_twitter(route, params)
        return data

            

    def get_user_info(self, uid_list):
        """Request user information, return a dictionary"""
        results = {}
        results['response'] = {}
        route = "users/show"
        for id in uid_list:
            params = {'user_id': id}
            data = self.__request_twitter(route, params)
            results['response'][ str(id)] = self.__format_user_info( data)
        return results

    def get_tweet_info(self, tweet_id):
        """Request tweet information, return a dictionary"""
        route = "statuses/lookup"
        params = {'id': tweet_id}
        data = self.__request_twitter(route, params)
        if data != []:
            return self.__format_tweet_info(data)
        else:
            return data

    # def get_retweeters( tweet_id):
    #     """Request the 100 last retweet ids, return them as a list"""
    #     data = api.request('statuses/retweeters/ids', { 'id': str(tweet_id)})
    #     response = {}
    #     response['response'] = data.json()['ids']
    #     retweeters = response['response']
    #     """change user_ids from num to string"""
    #     for index, num in enumerate(retweeters):
    #         retweeters[index] = str(num)
    #     return response

    # def get_tweet_data( tweet_id):
    #     """Request full tweet information, including retweet and user information"""
    #     url = "statuses/retweets/:%s" % tweet_id
    #     data = api.request(url, {'count': 100}).json()
    #     print( tweet_id)
    #     print( url)
    #     print("%s" % len(data))
    #     results = {}
    #     if len(data) == 0:
    #         results['response'] = []
    #     else:
    #         results['response'] = __format_tweet_data(data)
    #     return results

    # def get_user_timeline( user_id):
    #     """Get the latest tweets of a user.
    #     Returns up to 200 retweets in 4 categories."""

    #     tweets = __request_user_timeline( user_id)
    #     return tweets

    @staticmethod
    def __parse_properties( data, keys):
        response = {}
        for key in keys:
            response[key] = data[key]
        return response

    @staticmethod
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

    @staticmethod
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

    def __format_tweet_data(self, data):
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
        response['tweet_info'] = self.__parse_properties(tmp, tweet_info_keys)
        tmp = tmp['user']
        response['tweet_info']['user'] = self.__parse_properties(tmp, user_info_keys)

        for retweet in data:
            retweet_dict = self.__parse_properties(retweet, tweet_info_keys)
            tmp = retweet['user']
            retweet_dict['user'] = self.__parse_properties(tmp, user_info_keys)
            response['retweeter_ids'].append(tmp['id_str'])
            response['retweet_info'][tmp['id_str']] = retweet_dict
        return response
