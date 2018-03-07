from neo4j.v1 import GraphDatabase, basic_auth
from TwitterAPI import TwitterAPI
import json
import os
import time
import math
import random
import sty



class TwitterCrawler:
    LANGUAGES = ["de","en"];
    USERS_SHOW = "users/show";
    FOLLOWERS_IDS = "followers/ids";

    def __init__(self, q, lock):
        print("crawler is initialized with %s" % q)
        self.driver = GraphDatabase.driver(
            os.environ.get('NEO4J_URI'), auth=(
                os.environ.get('NEO4J_USER'),
                os.environ.get('NEO4J_PASSWORD')
            )
        )
        self.app_token = os.environ.get('APP_TOKEN')
        self.app_secret = os.environ.get('APP_SECRET')
        self.q = q
        self.lock = lock;
        # Get color for print background
        r = random.randint(0,150)
        g = random.randint(0,150)
        b = random.randint(0,150)
        self.color = sty.bg(r,g,b)
        self.__get_user_auth()
        self.run()

    def run(self):
        while True:
            if self.q.empty():
                print("queue empty... waiting...")
                time.sleep(10)
                continue
            user_id = self.q.get()
            self.crawl( user_id)


    def crawl(self, user_id):
        valid_int = self.__is_user_valid( user_id)
        if valid_int == 1:
            self.__delete_connections( user_id)
            print("%sCRAWLING:%s" % (self.color, user_id))
            self.__get_followers( user_id)
        elif valid_int == 0:
            self.__delete_user(user_id)
        else:
            self.__skip_user(user_id)

    def __is_user_valid(self, user_id):
        response = self.api.request(self.USERS_SHOW, {"user_id": user_id})
        parsed_response = json.loads(response.text)
        error_response = self.__check_error( parsed_response)
        if error_response:
            if error_response == 'invalid user':
                return 0
            elif error_response == 'continue':
                return __is_user_valid(user_id)
            else:
                print("%s%s" % (self.color, error_response))
                return 0
        elif self.__is_language_valid(parsed_response["lang"]) is False:
            return -1
        else:
            return 1

    def __is_language_valid(self, language):
        return language in self.LANGUAGES

    def __get_followers(self, user_id):
        cursor = -1
        num_followers = 0
        while cursor != 0:
            response = self.api.request(self.FOLLOWERS_IDS, {
                "cursor": cursor,
                "user_id": user_id,
                "count": 5000,
                "stringify_ids": "true"
            })
            parsed_response = json.loads(response.text)
            if "ids" in parsed_response and parsed_response["ids"] != []:
                followers += parsed_response["ids"]
                self.__save_user_followers(user_id, parsed_response["ids"])
            else:
                error_response = self.__check_error( parsed_response)
                if error_response:
                    if error_response == 'continue':
                        continue
                    elif error_response == 'invalid user':
                        self.__delete_user(user_id)
                        break
                    else:
                        print("%s%s:%s" % (self.color, error_response, user_id))
            if 'next_cursor' in parsed_response:
                cursor = parsed_response["next_cursor"]
            else:
                cursor = 0;
            num_followers += len(followers)

        self.__update_followers( user_id, num_followers)


    def __update_followers(self, user_id, num_followers):
        # Some verbosity about the kind of db write
        self.__finish_user_update(user_id)
        print("%sFINISHED:%s num_followers:%s" % 
            (self.color, user_id, num_followers))


    def __save_user_followers(self, user_id, followers):
        # Add a batch of follovers to the db
        query = "WITH %s AS followers " % followers
        query += "MERGE (u:USER{uid:'%s'}) " % user_id
        query += "FOREACH (follower IN followers | "
        query += "MERGE (f:USER{uid: follower}) "
        query += "ON CREATE SET f.timestamp=2 "
        query += "MERGE (u)<-[:FOLLOWS]-(f)) "

        with self.driver.session() as db:
            self.lock.acquire()
            db.run(query)
            self.lock.release()

    def __finish_user_update(self, user_id):
        timestamp = math.floor(time.time())
        query = "MATCH (a:USER:QUEUED{uid:'%s'}) " % user_id
        query += "SET a.timestamp=%s " % timestamp
        query += "REMOVE a:QUEUED"

        with self.driver.session() as db:
            self.lock.acquire()
            print("%sFINISH-WRITING:%s..." % (self.color, user_id))
            db.run(query)
            self.lock.release()


    def __delete_user(self, user_id):
        #delete invalid user and connections
        print("%sDELETING:%s" % (self.color, user_id))
        query = "MATCH (u:USER {uid: '" + user_id + "'}) "
        query += "DETACH DELETE u"
        with self.driver.session() as db:
            self.lock.acquire()
            db.run(query)
            self.lock.release()

    def __delete_connections(self, user_id):
        while True:
            print("%sREMOVING CONNECTIONS:%s" % (self.color, user_id))
            query = "MATCH (u:USER {uid: '%s' })<-[r:FOLLOWS]-() " % user_id
            query += "WITH r LIMIT 100000 "
            query += "DELETE r "
            query += "RETURN count(*)"
            with self.driver.session() as db:
                self.lock.acquire()
                result = db.run(query)
                self.lock.release()
                if result.single()[0] == 0:
                    break

    def __skip_user(self, user_id):
        #skipping users having the wrong language
        #by setting their timestamp to a high number
        present_time = math.floor(time.time())
        future_time = present_time + (60 * 60 * 24 * 365 * 10) #10 years in the future
        print("%sSKIPPING:%s" % (self.color, user_id))
        query = "MATCH (u:USER {uid: '%s'}) " % user_id
        query += "SET u.timestamp=%s " % future_time
        query += "REMOVE u:QUEUED"
        with self.driver.session() as db:
            self.lock.acquire()
            db.run(query)
            self.lock.release()

    def __check_error(self, response):
        error_response = "";
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
        # get new credentials from the db and block them
        # for 16m by setting the timestamp
        while True:
            user_token = ""
            user_secret = ""
            actual_time = math.floor(time.time())
            free_time = actual_time + (60 * 16)
            query = "MATCH (h:TOKEN) "
            query += "WHERE h.timestamp < %s " % actual_time
            query += "WITH h LIMIT 1 "
            query += "SET h.timestamp=%s " % free_time
            query += "RETURN h.token as token, "
            query += "h.secret as secret"
            with self.driver.session() as db:
                results = db.run(query)
                for user in results:
                    user_token = user["token"]
                    user_secret = user["secret"]
            if not user_token:
                print(user_token)
                print("%sAll token are busy, waiting..." % self.color)
                time.sleep(60)
                continue
            else:
                break
        self.api = TwitterAPI(
            self.app_token,
            self.app_secret,
            user_token,
            user_secret)
