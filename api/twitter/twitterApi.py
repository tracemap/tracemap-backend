import json
import time
import os
import re
import nltk
from nltk import word_tokenize
from nltk.corpus import stopwords


from TwitterAPI import TwitterAPI
from api.twitter.tokenProvider import Token

class TracemapTwitterApi:

    def __init__(self):
        try:
            nltk.download('stopwords')
        except FileExistsError:
            print('stopwords already exist')

    def __request_twitter(self, route: str, params: dict, route_extension: str="") -> dict:
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
            error_response = self.__check_error(api, parsed_response)
            if error_response:
                if error_response == 'continue':
                    continue
                else:
                    return {}
            else:
                return parsed_response

    def __check_error(self, api, response: dict) -> str:
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
                api.__get_user_auth()
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
            results['response'][ str(id)] = self.__format_user_info( data)
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
        params = { 'id': str(tweet_id)}
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
            'tweet_mode':'extended'
        }
        route = "statuses/user_timeline"
        data = self.__request_twitter(route, params)
        if data:
            response = {
                'tweets': [],
                'retweets': []
            }
        else:
            return {}
        for tweet in data:
            tm_tweet = {
                'id_str': "",
                'retweet_count': 0
            }
            word_dict = {}
            handle_dict = {}
            hashtag_dict = {}
            wordcloud = {
                'hashtags': [],
                'handles': [],
                'words': []
            }
            word_list = self.__get_word_list(tweet)
            for word in word_list:
                word = word.lower()
                if word in word_dict:
                    word_dict[word] += 1
                else:
                    word_dict[word] = 1
            for hashtag_obj in tweet['entities']['hashtags']:
                hashtag = '#%s' % hashtag_obj['text'].lower()
                if hashtag in hashtag_dict:
                    hashtag_dict[hashtag] += 1
                else:
                    hashtag_dict[hashtag] = 1
            for handle_obj in tweet['entities']['user_mentions']:
                handle = '@%s' % handle_obj['screen_name']
                if handle in handle_dict:
                    handle_dict[handle] += 1
                else:
                    hashtag_dict[handle] = 1
            wordcloud['words'] = word_dict
            wordcloud['hashtags'] = hashtag_dict
            wordcloud['handles'] = handle_dict
            tm_tweet['wordcloud'] = wordcloud
            if 'retweeted_status' in tweet and not tweet['retweeted_status']['user']['id_str'] == user_id:
                tm_tweet['id_str'] = tweet['retweeted_status']['id_str']
                tm_tweet['retweet_count'] = tweet['retweeted_status']['retweet_count']
                response['retweets'].append(tm_tweet)
            if 'retweeted_status' not in tweet:
                tm_tweet['id_str'] = tweet['id_str']
                tm_tweet['retweet_count'] = tweet['retweet_count']
                tm_tweet['full_text'] = tweet['full_text']
                response['tweets'].append(tm_tweet)
        return response


    def __get_word_list(self, tweet: dict) -> list:
        # replace any amount of whitespace/newline with a single space and split at space
        word_list = (re.sub(r'\s+', ' ', tweet['full_text'])).split(' ')
        # remove all nonword characters at eow
        word_list = [re.sub(r'\W+\B', '', word) for word in word_list]
        # filter all links, hashtags, handles
        word_list = [word for word in word_list if not any(seq in word for seq in ['www', 'http', 'https', '#', '@'])]
        # remove all words with nonword characters
        word_list = [re.sub(r'\W+', '', word) for word in word_list]
        # filter words smaller than 3 chars
        word_list = [word for word in word_list if len(word) > 2]
        word_list = self.__filter_stopwords(word_list, tweet['lang'])
        return word_list

    def __filter_stopwords(self, word_list: list, lang_short: str) -> list:
        language = self.__get_language(lang_short)
        stop = set(stopwords.words(language))
        return [word for word in word_list if word not in stop]

    @staticmethod
    def __get_language(lang_short: str) -> str:
        # maps the short bcp 47 language tag to
        # the nltk stopword lists language
        return {
            'ar': 'arabic',
            'da': 'danish',
            'nl': 'dutch',
            'en': 'english',
            'fi': 'finnish',
            'fr': 'french',
            'de': 'german',
            'el': 'greek',
            'it': 'italian',
            'nn': 'norwegian',
            'pt': 'portuguese',
            'br': 'portuguese',
            'ro': 'romanian',
            'ru': 'russian',
            'es': 'spanish',
            'sv': 'swedish',
            'tr': 'turkish'
        }.get(lang_short, "english")
        

    @staticmethod
    def __parse_properties( data, keys: list) -> dict:
        response = {}
        for key in keys:
            if key in data:
                response[key] = data[key]
        return response

    @staticmethod
    def __format_user_info( data: dict) -> dict:
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
    def __format_tweet_info( data: dict) -> dict:
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
     # The following values are lists
        tweet_dict["hashtags"] = data['entities']['hashtags']
        tweet_dict["user_mentions"] = data['entities']['user_mentions']
        return( response)

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
