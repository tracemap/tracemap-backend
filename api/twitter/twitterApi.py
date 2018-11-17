import json
import time
import os

from TwitterAPI import TwitterAPI
from api.twitter.tokenProvider import Token


class TracemapTwitterApi:

    def __request_twitter(self, route: str, params: dict, route_extension: str = "") -> dict:
        if not hasattr(self, route):
            token_instance = Token(route)
            setattr(self, route, token_instance)
        api = getattr(self, route).api
        while True:
            try:
                response = api.request("%s%s" % (route, route_extension), params)
            except Exception as exc:
                print("Error while requesting Twitter: %s" % exc)
                time.sleep(10)
                continue
            parsed_response = response.json()
            print(parsed_response)
            error_response = self.__check_error(token_instance, parsed_response)
            if error_response:
                if error_response == 'continue':
                    continue
                else:
                    return {}
            else:
                return parsed_response

    def __check_error(self, token_instance, response: dict) -> str:
        error_response = ""
        if 'error' in response:
            error_response = self.__check_twitter_error_code(
                response["error"])
        elif 'errors' in response:
            error_response = self.__check_twitter_error_code(
                response["errors"][0]["code"])
        if error_response == "":
            return ""
        else:
            if error_response == "Switch helper":
                token_instance.get_user_auth()

                return "continue"
            elif error_response in ("Invalid user", "Not authorized"):
                return "invalid user"
            else:
                return error_response

    @staticmethod
    def __check_twitter_error_code(code: int) -> str:
        return {
            32: "Switch helper",
            50: "Invalid user",
            63: "Invalid user",
            "Not authorized.": "Not authorized",
            88: "Switch helper",
            89: "Switch helper",
            131: "Internal error"
        }.get(code, "Unknown error %s" % code)

    def get_user_info(self, uid_list: list) -> dict:
        """Request user information, return a dictionary"""
        results = {'response': {}}
        route = "users/show"
        for id in uid_list:
            params = {'user_id': id}
            data = self.__request_twitter(route, params)
            results['response'][str(id)] = self.__format_user_info(data)
        return results

    def get_tweet_info(self, tweet_id: str) -> dict:
        """Request tweet information, return a dictionary"""
        route = "statuses/lookup"
        params = {'id': tweet_id}
        data = self.__request_twitter(route, params)
        if data != []:
            return self.__format_tweet_info(data)
        else:
            return data

    def get_retweeters(self, tweet_id: str) -> dict:
        """Request the 100 last retweet ids, return them as a list"""
        route = 'statuses/retweeters/ids'
        params = {'id': str(tweet_id)}
        data = self.__request_twitter(route, params)
        response = {}
        response['response'] = data['ids']
        retweeters = response['response']
        # change user_ids from num to string
        for index, num in enumerate(retweeters):
            retweeters[index] = str(num)
        return response

    def get_tweet_data(self, tweet_id: str) -> dict:
        """Request full tweet information, including retweet and user information"""
        route = "statuses/retweets"
        route_extension = '/:%s' % tweet_id
        params = {'count': 100}
        data = self.__request_twitter(route, params, route_extension)
        results = {}
        if len(data) == 0:
            results['response'] = []
        else:
            results['response'] = self.__format_tweet_data(data)
        return results

    def get_user_timeline(self, user_id: str) -> dict:
        """Get the latest tweets of a user.
        Returns last 200 retweets."""
        params = {
            'user_id': str(user_id),
            'exclude_replies': False,
            'count': 200,
            'tweet_mode': 'extended'
        }
        route = "statuses/user_timeline"
        data = self.__request_twitter(route, params)
        return data

    @staticmethod
    def __parse_properties(data, keys: list) -> dict:
        response = {}
        for key in keys:
            if key in data:
                response[key] = data[key]
        return response

    @staticmethod
    def __format_user_info(data: dict) -> dict:
        """Format get_user_info data as a dictionary of relevant data"""
        user_dict = {}
        user_dict["timestamp"] = str(time.time())
        user_dict["name"] = str(data['name'])
        user_dict["screen_name"] = str(data['screen_name'])
        user_dict["location"] = str(data['location'])
        user_dict["lang"] = str(data['lang'])
        user_dict["followers_count"] = int(data['followers_count'])
        user_dict["friends_count"] = int(data['friends_count'])
        user_dict["statuses_count"] = int(data['statuses_count'])
        user_dict["created_at"] = str(data['created_at'])
        user_dict["profile_image_url"] = str(data['profile_image_url'])
        return (user_dict)

    @staticmethod
    def __format_tweet_info(data: dict) -> dict:
        data = data[0]
        response = {}
        response['response'] = {}
        response['response'][data['id_str']] = {}
        tweet_dict = response['response'][data['id_str']]
        tweet_dict["reply_to"] = str(data['in_reply_to_status_id_str'])
        tweet_dict["lang"] = str(data['lang'])
        tweet_dict["author"] = str(data['user']['id_str'])
        tweet_dict["fav_count"] = str(data['favorite_count'])
        tweet_dict["retweet_count"] = str(data['retweet_count'])
        tweet_dict["date"] = str(data['created_at'])
        # The following values are lists
        tweet_dict["hashtags"] = data['entities']['hashtags']
        tweet_dict["user_mentions"] = data['entities']['user_mentions']
        return (response)

    def __format_tweet_data(self, data: dict) -> dict:
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
