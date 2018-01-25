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

        for user in self.__get_twitter_users_queue():
            response = self.__get_twitter_user_metadata(twitterHelper, user["id"])
            if type(response) == dict:
                self.__save_twitter_followers(twitterHelper, user["id"], response)
                self.__delete_twitter_user_from_queue(user["id"])
            elif response == "Switch helper" or response == "Cannot authenticate":
                if len(twitterHelpers) == 0:
                    print("Starting recursive crawling.")
                    self.start_crawling()
                else:
                    print("Switching twitter helper.")
                    twitterHelper = twitterHelpers.pop()
            elif response == "User not found" or response == "Suspended user":
                self.__delete_twitter_user_from_queue(user["id"])
                print("Deleting user id: " + user["id"] + " from users queue.")
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
                followersCount = len(parsedResponse["ids"])
                print(followersCount)
                query = ""
                followersQuery = ""
                for key, followerId in enumerate(parsedResponse["ids"]):
                    if len(followersQuery) > 8000:
                        self.db.run(followersQuery)
                        followersQuery = ""
                        print("cleared memory in the loop.")
                    followersQuery += self.__create_user_followers_batch_query(
                        key, userId, followerId, followersCount - 1, userData
                    )
                    query += self.__create_users_queue_batch_query(
                        key, followerId, followersCount - 1
                    )
                if len(followersQuery) > 0:
                    self.db.run(followersQuery)
                    print("executed after the loop.")
                self.db.run(query)
                print("User with id: " + userId + " and followers:" + str(followersCount) + " indexed.")
            elif "errors" in parsedResponse or "error" in parsedResponse:
                print(parsedResponse)
                break;

            cursor = parsedResponse["next_cursor_str"]

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
        results = self.db.run("MATCH (n:UsersQueue) RETURN n.id as id")
        for user in results:
            usersQueue.append({"id": user["id"]})

        return usersQueue

    def __validate_twitter_user(self, twitterHelper, userId):
        api = self.__get_twitter_api(twitterHelper)

        response = api.request(self.USERS_SHOW, {"user_id": userId})
        parsedResponse = json.loads(response.text)

        isValid = False
        if "status" in parsedResponse:
            isValid = self.__is_language_valid(parsedResponse["status"]["lang"])
        elif "errors" in parsedResponse:
            isValid = self.__check_twitter_error_code(
                parsedResponse["errors"][0]["code"]
            )

        return isValid

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
        query = "MATCH (n:UsersQueue {id: '" + id + "'}) "
        query += "DETACH DELETE n"
        response = self.db.run(query)
        print("Twitter user with id: ", id, " deleted from users queue.")

    def __create_users_queue_batch_query(self, key, followerId, followersCount):
        if key == 0 and followersCount == 0:
            key = "singleFollower"

        query = "MERGE (u"+str(key)+":USER:UsersQueue {id: '" + followerId + "'})"

        return {
            followersCount: query,
            "singleFollower": query
        }.get(key, query + " ")

    def __create_user_followers_query(
        self, userId, followerId, followersCount, userData):
        query = "MERGE (u:USER{uid:'" + userId + "',"
        query += " name: '" + userData["name"] + "',"
        query += " followers_count: " + followersCount + ","
        query += " created_at: TIMESTAMP()}) "
        query += "MERGE (f:USER{uid:'" + followerId + "'}) "
        query += "MERGE (u)<-[:FOLLOWS]-(f)"

        return query

    def __create_user_followers_batch_query(
        self, key, userId, followerId, followersCount, userData):

        if key == 0 and followersCount == 0:
            key = "singleFollower"

        query = "MERGE (u"+str(key)+":USER{uid:'" + userId + "'})"
        query += " SET u"+str(key)+" += {name: '" + userData["name"] + "',"
        query += " followers_count: " + str(followersCount) + ","
        query += " created_at: TIMESTAMP()} "
        query += "MERGE (f"+str(key)+":USER{uid:'" + followerId + "'}) "
        query += "MERGE (u"+str(key)+")<-[:FOLLOWS]-(f"+str(key)+")"

        return {
            followersCount: query,
            "singleFollower": query
        }.get(key, query + " ")

    def __get_twitter_api(self, twitterHelper):
        return TwitterAPI(
            self.token,
            self.secret,
            twitterHelper["accessToken"],
            twitterHelper["accessTokenSecret"])

tc = TwitterCrawler()
tc.start_crawling()