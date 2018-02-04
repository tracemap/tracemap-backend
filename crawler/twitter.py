from neo4j.v1 import GraphDatabase, basic_auth
from TwitterAPI import TwitterAPI
import json
import os


class TwitterCrawler:
    GERMAN = "de";
    USERS_SHOW = "users/show";
    FOLLOWERS_IDS = "followers/ids";

    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.environ.get('neo4j_url'), auth=(os.environ.get('neo4j_username'), os.environ.get('neo4j_password'))
        )
        self.db = self.driver.session()
        self.token = os.environ.get('APP_TOKEN')
        self.secret = os.environ.get('APP_SECRET')

    def start_crawling(self):
        temporary_users_queue = ["87223459", "708559276596453376", "14910961", "3246151614", "3300472738", "519152261"]
        twitter_helpers = self.__get_twitter_helpers()
        twitter_helper = twitter_helpers.pop()

        for user_id in temporary_users_queue:
            response = self.__get_twitter_user_metadata(twitter_helper, user_id)
            if type(response) == dict:
                followers_response = self.__save_twitter_followers(twitter_helper, user_id, response)
                if followers_response is False:
                    if len(twitter_helpers) == 0:
                        self.start_crawling()
                    else:
                        twitter_helper = twitter_helpers.pop()
            elif response == "Switch helper" or \
                    response == "Cannot authenticate":
                if len(twitter_helpers) == 0:
                    self.start_crawling()
                else:
                    twitter_helpers = twitter_helpers.pop()
            elif response is not False:
                print(response)

    def __save_twitter_followers(self, twitter_helper, user_id, user_data):
        api = self.__get_twitter_api(twitter_helper)

        cursor = -1
        followers = []
        while cursor != 0:
            response = api.request(self.FOLLOWERS_IDS, {
                "cursor": cursor,
                "user_id": user_id,
                "count": 5000,
                "stringify_ids": "true"
            })
            parsed_response = json.loads(response.text)
            if "ids" in parsed_response and parsed_response["ids"] != []:
                followers += parsed_response["ids"]
            elif "errors" in parsed_response or "error" in parsed_response:
                error_response = ""
                if "error" in parsed_response:
                    error_response = self.__check_twitter_error_code(
                        parsed_response["error"]
                    )
                else:
                    error_response = self.__check_twitter_error_code(
                        parsed_response["errors"][0]["code"]
                    )
                if error_response == "Switch helper" or \
                        error_response == "Cannot authenticate" or \
                        error_response == "Unknown error":
                    return False

            cursor = parsed_response["next_cursor"]

        if len(followers) != 0:
            with self.driver.session() as session:
                indexed_followers = self.__get_user_followers(user_id)
                difference = set(indexed_followers) - set(followers)
                log_message = "User ID: " + user_id + " indexed with " + str(len(followers)) + " indexed."
                if len(difference) > 0 and len(indexed_followers) != 0:
                    session.write_transaction(self.__delete_user, user_id)
                    session.write_transaction(self.__save_user_followers, user_id, followers, user_data)
                    print(log_message)
                elif len(indexed_followers) == 0:
                    session.write_transaction(self.__save_user_followers, user_id, followers, user_data)
                    print(log_message)

        return True

    @staticmethod
    def __delete_user(tx, user_id):
        query = "MATCH (u:USER {uid: '" + user_id + "'}) "
        query += "DETACH DELETE u"
        tx.run(query)

    def __get_twitter_helpers(self):
        twitter_credentials = []
        query = "MATCH (h:TwitterHelper) "
        query += "RETURN h.userName as userName, "
        query += "h.accessToken as accessToken, "
        query += "h.accessTokenSecret as accessTokenSecret"
        results = self.db.run(query)

        for user in results:
            twitter_credentials.append(
                {"userName": user["userName"],
                 "accessToken": user["accessToken"],
                 "accessTokenSecret": user["accessTokenSecret"]}
            )

        return twitter_credentials

    def __get_twitter_user_metadata(self, twitter_helper, user_id):
        api = self.__get_twitter_api(twitter_helper)
        response = api.request(self.USERS_SHOW, {"user_id": user_id})
        parsed_response = json.loads(response.text)
        if "errors" in parsed_response or "error" in parsed_response:
            parsed_response = self.__check_twitter_error_code(
                parsed_response["errors"][0]["code"]
            )
        elif self.__is_language_valid(parsed_response["status"]["lang"]) is False:
            parsed_response = False

        return parsed_response

    def __get_user_followers(self, user_id):
        current_followers = []
        query = "MATCH (n:USER)<-[:FOLLOWS]-(m) WHERE n.uid = '" + user_id + "' RETURN m.uid as id"
        followers = self.db.run(query)
        for follower in followers:
            current_followers.append(follower["id"])

        return current_followers

    @staticmethod
    def __save_users_to_queue(tx, followers):
        query = "WITH " + str(followers) + " AS followers "
        query += "FOREACH (follower IN followers | "
        query += "MERGE (q:USER:UsersQueue {id: follower}))"

        tx.run(query)

    @staticmethod
    def __save_user_followers(tx, user_id, followers, user_data):
        query = "WITH " + str(followers) + " AS followers "
        query += "FOREACH (follower IN followers | "
        query += "MERGE (u:USER{uid:'" + user_id + "'})"
        query += ' SET u += {name: "' + user_data["name"] + '",'
        query += " friends_count: " + str(user_data["friends_count"]) + ","
        query += " profile_image_url: '" + user_data["profile_image_url"] + "',"
        query += ' screen_name: "' + user_data["screen_name"] + '",'
        query += " statuses_count: " + str(user_data["statuses_count"]) + ","
        query += " verified: " + str(user_data["verified"]) + ","
        query += " followers_count: " + str(len(followers)) + ","
        query += " created_at: TIMESTAMP()} "
        query += "MERGE (f:USER{uid: follower}) "
        query += "MERGE (u)<-[:FOLLOWS]-(f))"

        tx.run(query)

    def __is_language_valid(self, language):
        return {
            self.GERMAN: True
        }.get(language, False)

    @staticmethod
    def __check_twitter_error_code(code):
        return {
            32: "Cannot authenticate",
            50: "User not found",
            63: "Suspended user",
            88: "Switch helper",
            89: "Switch helper",
            131: "Internal error"
        }.get(code, "Unknown error")

    def __get_twitter_api(self, twitter_helper):
        return TwitterAPI(
            self.token,
            self.secret,
            twitter_helper["accessToken"],
            twitter_helper["accessTokenSecret"])


tc = TwitterCrawler()
tc.start_crawling()
