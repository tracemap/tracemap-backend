from neo4j.v1 import GraphDatabase, basic_auth
from TwitterAPI import TwitterAPI
import json
import os
import time
import math
import random
import sty


class TwitterCrawler:
    LANGUAGES = ["de", "en"];
    USERS_SHOW = "users/show";
    FOLLOWERS_IDS = "followers/ids";
    RATE_LIMIT = "application/rate_limit_status"

    def __init__(self, q, log_q):
        print("crawler is initialized with %s" % q)
        while True:
            try:
                self.driver = GraphDatabase.driver(
                    os.environ.get('NEO4J_URI'), auth=(
                        os.environ.get('NEO4J_USER'),
                        os.environ.get('NEO4J_PASSWORD')
                    )
                )
                print("crawler connected to the database.")
                break
            except Exception as exc:
                print("Exception: %s -> crawler could not connect to the database. Retrying in 5s..." % exc)
                time.sleep(5)
                continue
        self.app_token = os.environ.get('APP_TOKEN')
        self.app_secret = os.environ.get('APP_SECRET')
        self.q = q
        self.log_q = log_q

        # Eliminating the use of locks for now
        # self.lock = lock;

        # Get color for print background
        r = random.randint(0, 150)
        g = random.randint(0, 150)
        b = random.randint(0, 150)
        self.color = sty.bg(r, g, b)
        self.__get_user_auth()
        self.run()

    def run(self):
        while True:
            # Is this verification necessary?
            if self.q.empty():
                print("queue empty... waiting 10s...")
                time.sleep(10)
                continue
            user_id = self.q.get()
            self.crawl(user_id)

    def crawl(self, user_id):
        valid_int = self.__is_user_valid(user_id)
        if valid_int == 1:
            self.__delete_connections(user_id)
            print("%sCRAWLING: %s" % (self.color, user_id))
            self.__get_followers(user_id)
        elif valid_int == 0:
            self.__delete_user(user_id)
        else:
            self.__skip_user(user_id)

    def __is_user_valid(self, user_id):
        while True:
            try:
                response = self.api.request(self.USERS_SHOW, {"user_id": user_id})
                break
            except Exception as exc:
                print("%s###ERROR### -> %s: ConnectTimeout retrying in 10s..." % (self.color, exc))
                time.sleep(10)
                continue
        parsed_response = json.loads(response.text)
        error_response = self.__check_error(parsed_response)
        if error_response:
            if error_response == 'invalid user':
                return 0
            elif error_response == 'continue':
                return self.__is_user_valid(user_id)
            else:
                print("%s%s" % (self.color, error_response))
                return 0
        elif not self.__is_language_valid(parsed_response["lang"]):
            return -1
        else:
            return 1

    def __is_language_valid(self, language):
        # will there be no language check anymore?
        # return language in self.LANGUAGES
        return True

    def __get_followers(self, user_id):
        cursor = -1
        while cursor != 0:
            while True:
                try:
                    response = self.api.request(self.FOLLOWERS_IDS, {
                        "cursor": cursor,
                        "user_id": user_id,
                        "count": 5000,
                        "stringify_ids": "true"
                    })
                    break
                except Exception as exc:
                    print("%s###ERROR### -> %s: ConnectTimeout retrying in 10s..." %
                          (self.color, exc))
                    time.sleep(10)
                    continue
            parsed_response = json.loads(response.text)
            if "ids" in parsed_response and parsed_response["ids"] != []:
                self.__save_user_followers(user_id, parsed_response["ids"])
            else:
                error_response = self.__check_error(parsed_response)
                if error_response:
                    if error_response == 'continue':
                        # If it was a user_token problem, it should retry to save followers
                        # __get_followers(user_id)
                        # return
                        print("%s Saving followers failed, new token acquired: " % self.color)
                        if 'next_cursor' in parsed_response:
                            print("%s If this number -> %s <- is zero, function will not get followers!!!" %
                                  (self.color, parsed_response["next_cursor"]))
                        else:
                            print("DANGER!!! exiting the function without getting followers!!!!!!!!!!")
                        continue
                    elif error_response == 'invalid user':
                        self.__delete_user(user_id)
                        break
                    else:
                        print("%s%s:%s" % (self.color, error_response, user_id))
            if 'next_cursor' in parsed_response:
                cursor = parsed_response["next_cursor"]
            else:
                cursor = 0

        self.__update_followers(user_id, 0)

    def __update_followers(self, user_id, num_followers):
        # Some verbosity about the kind of db write
        self.__finish_user_update(user_id)
        print("%sFINISHED:%s num_followers:%s" % 
              (self.color, user_id, num_followers))

    def __save_user_followers(self, user_id, followers):
        # Add a batch of followers to the db
        present_time = math.floor(time.time())
        query = "WITH %s AS followers " % followers
        query += "MATCH (u:USER:QUEUED{uid:'%s'}) " % user_id
        query += "FOREACH (follower IN followers | "
        query += "MERGE (f:USER{uid: follower}) "
        query += "ON CREATE SET f.timestamp=%s, f:PRIORITY2" % present_time
        query += "MERGE (u)<-[:FOLLOWS]-(f)) "
        self.__run_query(query)

    def __finish_user_update(self, user_id):
        timestamp = math.floor(time.time())
        query = "MATCH (a:USER:QUEUED{uid:'%s'}) " % user_id
        query += "SET a.timestamp=%s, a:PRIORITY3" % timestamp
        query += "REMOVE a:QUEUED"
        print("%sFINISH-WRITING:%s..." % (self.color, user_id))
        # write log in log_q, which info?
        self.__run_query(query)

    def __delete_user(self, user_id):
        # delete invalid user and connections
        print("%sDELETING:%s" % (self.color, user_id))
        query = "MATCH (u:USER:QUEUED{uid:'%s'}) " % user_id
        query += "DETACH DELETE u"
        self.__run_query(query)

    def __delete_connections(self, user_id):
        while True:
            query = "MATCH (u:USER:QUEUED{uid: '%s' })<-[r:FOLLOWS]-() " % user_id
            query += "WITH r LIMIT 100000 "
            query += "DELETE r "
            query += "RETURN count(*)"
            result = self.__run_get_query(query)
            if result == 0:
                break

    def __skip_user(self, user_id):
        # skipping users having the wrong language
        # by setting their timestamp to a high number
        present_time = math.floor(time.time())
        future_time = present_time + (60 * 60 * 24 * 365 * 10)  # 10 years in the future
        print("%sSKIPPING: %s" % (self.color, user_id))
        query = "MATCH (u:USER:QUEUED{uid: '%s'}) " % user_id
        query += "SET u.timestamp=%s, u:PRIORITY3 " % future_time
        query += "REMOVE u:QUEUED"
        self.__run_query(query)

    def __check_error(self, response):
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
                self.__get_user_auth()
                return "continue"
            elif error_response == "Invalid user":
                return "invalid user"
            elif error_response == "Not authorized":
                return "invalid user"
            else:
                return error_response

    def __run_query(self, query):
        with self.driver.session() as db:
            while True:
                try:
                    db.run(query)
                    break
                except Exception as exc:
                    e_name = type(exc).__name__
                    if e_name == "TransientError":
                        print("%s######\nDB DATA IS LOCKED. Retrying...######\n######" % self.color)
                        print("%s" % exc)
                        time.sleep(2)
                        continue
                    else:                    
                        print("%s######\n\n\n######\n\n\n%s\n\n\n%s" % (self.color, type(exc).__name__, exc))

    def __run_get_query(self, query):
        with self.driver.session() as db:
            while True:
                try:
                    result = db.run(query)
                    break
                except Exception as exc:
                    e_name = type(exc).__name__
                    if e_name == "TransientError":
                        print("%s######\nDB DATA IS LOCKED. Retrying...######\n######" % self.color)
                        print("%s" % exc)
                        time.sleep(2)
                        continue
                    else:                    
                        print("%s######\n\n\n######\n\n\n%s\n\n\n%s" % (self.color, type(ex).__name__, ex))
            return result.single()[0]


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

    def __get_user_auth(self):
        if hasattr(self, 'api'):
            self.__update_reset_time()
        # get new credentials from the db and block them
        # for 16m by setting the timestamp
        user_token = ""
        user_secret = ""
        while True:
            actual_time = math.floor(time.time())
            guessed_free_time = actual_time + (60 * 16)
            query = "MATCH (h:TOKEN) "
            query += "WHERE h.timestamp < %s " % actual_time
            query += "WITH h LIMIT 1 "
            query += "SET h.timestamp=%s " % guessed_free_time
            query += "RETURN h.token as token, "
            query += "h.secret as secret"
            with self.driver.session() as db:
                results = db.run(query)
                for user in results:
                    user_token = user["token"]
                    user_secret = user["secret"]
            if not user_token:
                print(user_token)
                print("All token are busy, waiting 30s...")
                time.sleep(30)
                continue
            else:
                break
        self.user_token = user_token
        self.api = TwitterAPI(
            self.app_token,
            self.app_secret,
            user_token,
            user_secret)

    def __update_reset_time(self):
        # update token's timestamp to real reset time
        while True:
            try:
                response = self.api.request(self.RATE_LIMIT, {
                    "resources": "followers"
                })
                break
            except Exception as exc:
                print("%s###ERROR### -> %s: ConnectTimeout retrying in 10s..." % (self.color, exc))
                time.sleep(10)
                continue
        parsed_response = json.loads(response.text)
        try:
            reset_time = parsed_response['resources']['followers']['/followers/ids']['reset']
            query = "MATCH (h:TOKEN{token:'%s'}) " % self.user_token
            query += "SET h.timestamp=%s" % reset_time
            self.__run_query(query)
        except Exception as exc:
            print("%s###ERROR### -> %s: Failed to reset the token's timestamp" % (self.color, exc))
            if 'errors' in parsed_response:
                print("parsed_response: %s" % parsed_response)
                e_code = parsed_response['errors'][0]['code']
                if e_code == 89:
                    query = "MATCH (h:TOKEN{token:'%s'}) " % self.user_token
                    query += "DELETE h"
                    self.__run_query(query)
                    print("Error corrected, token's timestamp updated!")
            else:
                print("parsed_response: %s" % parsed_response)
