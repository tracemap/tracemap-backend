from neo4j.v1 import GraphDatabase
from TwitterAPI import TwitterAPI
import json
import os
import time
import math


class Crawler:
    USERS_SHOW = "users/show"
    FOLLOWERS_IDS = "followers/ids"
    RATE_LIMIT = "application/rate_limit_status"

    def __init__(self, q, name):
        self.q = q
        self.name = name
        self.languages = ["de", "en"]
        self.__log_to_file(self.name + " is initialized.")

        self.driver = self.__connect_to_db()

        self.app_token = os.environ.get('APP_TOKEN')
        self.app_secret = os.environ.get('APP_SECRET')

        self.__get_user_auth()
        self.run()

    def __connect_to_db(self):
        while True:
            try:
                driver = GraphDatabase.driver(
                    os.environ.get('NEO4J_URI'), auth=(
                        os.environ.get('NEO4J_USER'),
                        os.environ.get('NEO4J_PASSWORD')
                    )
                )
                self.__log_to_file(self.name + " connected to the database.")
                return driver
            except Exception as exc:
                self.__log_to_file("0 - ERROR -> %s. Crawler could not connect to the database. Retrying in 5s..." % exc)
                time.sleep(5)
                continue

    def run(self):
        empty_state = True if self.q.empty() else False
        while True:
            if self.q.empty():
                if not empty_state:
                    self.__log_to_file("Queue empty. Waiting...\n\n\n")
                    empty_state = True
                time.sleep(10)
                continue
            empty_state = False
            user_id = self.q.get()
            self.crawl(user_id)

    def crawl(self, user_id):
        valid_int = self.__is_user_valid(user_id)
        if valid_int == 1:
            self.__log_to_file("CRAWLING: %s" % user_id)
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
                self.__log_to_file("1 - ERROR -> %s. ConnectTimeout retrying in 10s..." % exc)
                time.sleep(10)
                continue
        parsed_response = json.loads(response.text)
        error_response = self.__check_error(parsed_response)
        if error_response:
            if error_response == 'invalid user':
                self.__log_to_file("User is invalid!")
                return 0
            elif error_response == 'continue':
                return self.__is_user_valid(user_id)
            else:
                self.__log_to_file("2 - UNKNOWN ERROR -> %s." % error_response)
                return 0
        else:
            self.__log_to_file("User is validated for crawling!")
            return 1

    def __get_followers(self, user_id):
        delete = False
        cursor = -1
        num_followers = 0
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
                    self.__log_to_file("3 - ERROR -> %s. ConnectTimeout retrying in 10s..." % exc)
                    time.sleep(10)
                    continue
            parsed_response = json.loads(response.text)
            if "ids" in parsed_response:
                self.__save_user_followers(user_id, parsed_response["ids"])
                num_followers += len(parsed_response["ids"])
            else:
                error_response = self.__check_error(parsed_response)
                if error_response:
                    if error_response == 'continue':
                        continue
                    elif error_response == 'invalid user':
                        self.__delete_user(user_id)
                        delete = True
                        break
                    else:
                        self.__log_to_file("6 - UNKNOWN ERROR -> %s (while trying to get followers from user %s). "
                                           "parsed_response = %s" % (error_response, user_id, parsed_response))
            if 'next_cursor' in parsed_response:
                cursor = parsed_response["next_cursor"]
            else:
                cursor = 0

        if not delete:
            self.__finish_update(user_id, num_followers)

    def __save_user_followers(self, user_id, followers):
        # Add a batch of followers to the db
        line = ''
        for follower in followers:
            line += follower
            line += ','
        if line != '':
            line = line[:-1]
        with open("temp/temp_%s.txt" % user_id, "a") as temp_file:
            temp_file.write(line+'\n')
        self.__log_to_file("Batch of followers of user %s saved to file." % user_id)

    def __finish_update(self, user_id, num_followers):
        # Some verbosity about the kind of db write
        os.rename("temp/temp_%s.txt" % user_id, "temp/%s_save.txt" % user_id)
        self.__log_to_file("User %s crawling complete. File ready to br processed by writer: num_followers: %s\n\n\n" %
                           (user_id, num_followers))

    def __delete_user(self, user_id):
        # delete invalid user and connections
        with open("temp/%s_delete.txt" % user_id, "a") as temp_file:
            temp_file.write('')
        self.__log_to_file("User %s ready to be deleted.\n\n\n" % user_id)

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
                self.__log_to_file("7 - Unusual error response: %s" % error_response)
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
                        self.__log_to_file("8 - ERROR -> %s. DB data is locked. Retrying..." % e_name)
                        self.__log_to_file(str(exc))
                        time.sleep(2)
                        continue
                    elif e_name == "AddressError":
                        self.driver = self.__connect_to_db()
                    else:
                        self.__log_to_file("9 - UNKNOWN ERROR -> %s." % e_name)
                        time.sleep(2)
                        continue

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
            query += "WHERE NOT h:BUSYTOKEN "
            query += "AND h.timestamp < %s " % actual_time
            query += "WITH h LIMIT 1 "
            query += "SET h.timestamp=%s " % guessed_free_time
            query += "SET h:BUSYTOKEN "
            query += "RETURN h.token as token, "
            query += "h.secret as secret, "
            query += "h.user as user"
            with self.driver.session() as db:
                results = db.run(query)
                for user in results:
                    user_token = user["token"]
                    user_secret = user["secret"]
                    user_name = user["user"]
            if not user_token:
                self.__log_to_file("All tokens are busy, waiting 30s...")
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
        self.__log_to_file("##### Saving followers failed, new token acquired. Using token from %s" % user_name)

    def __update_reset_time(self):
        # update token's timestamp to real reset time
        while True:
            try:
                response = self.api.request(self.RATE_LIMIT, {
                    "resources": "followers"
                })
                break
            except Exception as exc:
                self.__log_to_file("13 - ERROR -> %s. ConnectTimeout retrying in 10s..." % exc)
                time.sleep(10)
                continue
        parsed_response = json.loads(response.text)
        try:
            reset_time = parsed_response['resources']['followers']['/followers/ids']['reset']
            query = "MATCH (h:BUSYTOKEN{token:'%s'}) " % self.user_token
            query += "REMOVE h:BUSYTOKEN "
            query += "SET h.timestamp=%s" % reset_time
            self.__run_query(query)
        except Exception as exc:
            self.__log_to_file("14 - ERROR -> %s. Failed to reset the token's timestamp." % exc)
            if 'errors' in parsed_response:
                e_code = parsed_response['errors'][0]['code']
                if e_code == 89:
                    query = "MATCH (h:BUSYTOKEN{token:'%s'}) " % self.user_token
                    query += "REMOVE h:BUSYTOKEN SET h:OLDTOKEN"
                    self.__run_query(query)
                    self.__log_to_file("Error corrected, token deleted!")
                else:
                    self.__log_to_file("Unknown error in __update_reset_time() function")
            else:
                self.__log_to_file("Unknown error in _update_reset_time(). parsed_response = %s" % parsed_response)

    def __log_to_file(self, message):
        # print(message)
        now = time.strftime("[%a, %d %b %Y %H:%M:%S] ", time.localtime())
        with open("log/"+self.name+".log", 'a') as log_file:
            log_file.write(now + message + '\n')
