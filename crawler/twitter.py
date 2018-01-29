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
            os.environ.get('neo4j_url'),
            auth=(
                os.environ.get('neo4j_username'),
                os.environ.get('neo4j_password')
            )
        )
        self.db = self.driver.session()
        self.token = os.environ.get('APP_TOKEN')
        self.secret = os.environ.get('APP_SECRET')

    def start_crawling(self):
        twitterHelpers = self.__get_twitter_helpers()
        twitterHelper = twitterHelpers.pop()

        for userId in self.__get_twitter_users_queue():
            response = self.__get_twitter_user_metadata(twitterHelper, userId)
            if type(response) == dict:
                followersResponse = self.__save_twitter_followers(twitterHelper, userId, response)
                if followersResponse == False:
                    if len(twitterHelpers) == 0:
                        print("Starting recursive crawling.")
                        self.start_crawling()
                    else:
                        print("Switching twitter helper.")
                        twitterHelper = twitterHelpers.pop()
                else:
                    self.__delete_twitter_user_from_queue(userId)
            elif response == "Switch helper" or response == "Cannot authenticate":
                if len(twitterHelpers) == 0:
                    print("Starting recursive crawling.")
                    self.start_crawling()
                else:
                    print("Switching twitter helper.")
                    twitterHelper = twitterHelpers.pop()
            elif response == "User not found" or response == "Suspended user":
                self.__delete_twitter_user_from_queue(userId)
                print("Deleting user id: " + userId + " from users queue.")
            elif response is not False:
                print(response)

    def __save_twitter_followers(self, twitterHelper, userId, userData):
        api = self.__get_twitter_api(twitterHelper)

        cursor = -1
        while cursor == -1:
            response = api.request(self.FOLLOWERS_IDS, {
                "cursor": cursor,
                "user_id": userId,
                "count": 5000,
                "stringify_ids": "true"
            })
            parsedResponse = json.loads(response.text)
            if "ids" in parsedResponse and parsedResponse["ids"] != []:
                followersQuery = self.__create_user_followers_query(
                    userId, parsedResponse, userData
                )
                self.db.run(followersQuery)
                print("User with id: " + userId + " and " + str(len(parsedResponse["ids"])) + " followers was indexed.")
            elif "errors" in parsedResponse or "error" in parsedResponse:
                errorResponse = ""
                if "error" in parsedResponse:
                    errorResponse = self.__check_twitter_error_code(
                        parsedResponse["error"]
                    )
                else:
                    errorResponse = self.__check_twitter_error_code(
                        parsedResponse["errors"][0]["code"]
                    )
                if errorResponse == "Switch helper" or errorResponse == "Cannot authenticate" or errorResponse == "Unknown error":
                    return False

            cursor = parsedResponse["next_cursor_str"]

        return True

    def __get_twitter_helpers(self):
        twitterCredentials = []
        query = "MATCH (h:TwitterHelper) "
        query += "RETURN h.userName as userName, "
        query += "h.accessToken as accessToken, "
        query += "h.accessTokenSecret as accessTokenSecret"
        results = self.db.run(query)

        for user in results:
            twitterCredentials.append(
                {"userName": user["userName"],
                "accessToken": user["accessToken"],
                "accessTokenSecret": user["accessTokenSecret"]}
            )

        return twitterCredentials

    def __get_twitter_users_queue(self):
        usersQueue = []
        results = self.db.run("MATCH (n:USER:UsersQueue) RETURN n.id as id")
        for user in results:
            usersQueue.append(user["id"])

        return usersQueue

    def __get_twitter_user_metadata(self, twitterHelper, userId):
        api = self.__get_twitter_api(twitterHelper)
        response = api.request(self.USERS_SHOW, {"user_id": userId})
        parsedResponse = json.loads(response.text)
        if "errors" in parsedResponse or "error" in parsedResponse:
            parsedResponse = self.__check_twitter_error_code(
                parsedResponse["errors"][0]["code"]
            )

        return parsedResponse

    def __is_language_valid(self, language):
        return {
            self.GERMAN: True
        }.get(language, False)

    def __check_twitter_error_code(self, code):
        return {
            32: "Cannot authenticate",
            50: "User not found",
            63: "Suspended user",
            88: "Switch helper",
            89: "Switch helper",
            131: "Internal error"
        }.get(code, "Unknown error")

    def __delete_twitter_user_from_queue(self, id):
        query = "MATCH (u:USER:UsersQueue {id: '" + id + "'}) "
        query += "DETACH DELETE u"
        response = self.db.run(query)
        print("Twitter user with id: ", id, " deleted from users queue.")

    def __create_user_followers_query(self, userId, followers, userData):
        query = "WITH " + str(followers["ids"]) + " AS followers "
        query += "FOREACH (follower IN followers | "
        query += "MERGE (u:USER{uid:'" + userId + "'})"
        query += " SET u += {name: '" + userData["name"] + "',"
        query += " friends_count: '" + str(userData["friends_count"]) + "',"
        query += " profile_image_url: '" + userData["profile_image_url"] + "',"
        query += " screen_name: '" + userData["screen_name"] + "',"
        query += " statuses_count: '" + str(userData["statuses_count"]) + "',"
        query += " verified: '" + str(userData["verified"]) + "',"
        query += " followers_count: " + str(len(followers)) + ","
        query += " created_at: TIMESTAMP()} "
        query += "MERGE (f:USER{uid: follower}) "
        query += "MERGE (u)<-[:FOLLOWS]-(f) "
        query += "MERGE (q:USER:UsersQueue {id: follower}))"

        return query

    def __get_twitter_api(self, twitterHelper):
        return TwitterAPI(
            self.token,
            self.secret,
            twitterHelper["accessToken"],
            twitterHelper["accessTokenSecret"])

tc = TwitterCrawler()
tc.start_crawling()
