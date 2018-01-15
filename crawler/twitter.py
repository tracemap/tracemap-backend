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
            response = self.__validate_twitter_user(twitterHelper, user["id"])
            if response is True:
                self.__save_twitter_followers(twitterHelper, user["id"])
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

    def __save_twitter_followers(self, twitterHelper, userId):
        api = TwitterAPI(
            self.token,
            self.secret,
            twitterHelper["accessToken"],
            twitterHelper["accessTokenSecret"]
        )

        cursor = -1
        while cursor == -1:
            response = api.request(self.FOLLOWERS_IDS, {
                "cursor": cursor,
                "user_id": userId,
                "count": 5000,
                "stringify_ids": "true"
            })

            parsedResponse = json.loads(response.text)
            if "ids" in parsedResponse:
                followersCount = len(parsedResponse["ids"])
                query = ""
                followersQuery = ""
                for key, followerId in enumerate(parsedResponse["ids"]):
                    followersQuery += self.__create_user_followers_batch_query(
                        key, userId, followerId, followersCount - 1
                    )
                    query += self.__create_users_queue_batch_query(
                        key, followerId, followersCount - 1
                    )
                self.db.run(followersQuery)
                self.db.run(query)
                print("User with id: " + userId + " indexed.")
            elif "errors" in parsedResponse:
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
        api = TwitterAPI(
            self.token,
            self.secret,
            twitterHelper["accessToken"],
            twitterHelper["accessTokenSecret"])

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

        query = "(:UsersQueue {id: '" + followerId + "'})"

        return {
            0: "CREATE " + query + ", ",
            followersCount: query,
            "singleFollower": "CREATE " + query
        }.get(key, query + ", ")

    def __create_user_followers_query(self, userId, followerId):
        query = "MERGE (u:TwitterUser{id:'" + userId + "'}) "
        query += "MERGE (f:TwitterUser{id:'" + followerId + "'}) "
        query += "MERGE (u)<-[:FOLLOWS]-(f)"

        return query

    def __create_user_followers_batch_query(
        self, key, userId, followerId, followersCount):

        if key == 0 and followersCount == 0:
            key = "singleFollower"

        query = "MERGE (u"+str(key)+":TwitterUser{id:'" + userId + "'}) "
        query += "MERGE (f"+str(key)+":TwitterUser{id:'" + followerId + "'}) "
        query += "MERGE (u"+str(key)+")<-[:FOLLOWS]-(f"+str(key)+")"
        return {
            followersCount: query,
            "singleFollower": query
        }.get(key, query + " ")

tc = TwitterCrawler()
tc.start_crawling()
