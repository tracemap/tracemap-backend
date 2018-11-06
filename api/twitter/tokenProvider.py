from neo4j.v1 import GraphDatabase
from TwitterAPI import TwitterAPI
import json
import os
import time
import math


class Token:

    RATE_LIMIT = "application/rate_limit_status"

    def __init__(self, twitter_route: str):
        self.twitter_route = twitter_route
        self.neo4j_driver = self.__connect_to_db()
        self.app_token = os.environ.get('APP_TOKEN')
        self.app_secret = os.environ.get('APP_SECRET')
        self.__cleanup_last_session()
        self.__get_user_auth()

    def __connect_to_db(self):
        while True:
            try:
                driver = GraphDatabase.driver(
                    os.environ.get('NEO4J_URI'), auth=(
                        os.environ.get('NEO4J_USER'),
                        os.environ.get('NEO4J_PASSWORD')
                    )
                )
                return driver
            except Exception as exc:
                print("ERROR on neo4j driver initialization -> %s" % exc)
                time.sleep(5)
                continue

    def __get_user_auth(self):
        """
        Return old token by updating the reset time
        and get a free token.
        """
        if hasattr(self, 'api'):
            self.__update_reset_time()
        # get new credentials from the db and block them
        # for 1000min by setting the timestamp
        user_token = ""
        user_secret = ""
        while True:
            actual_time = math.floor(time.time())
            distant_time = actual_time + (60 * 1000)
            query = "MATCH (h:TOKEN) "
            query += "WHERE NOT EXISTS(h.`%s`) " % self.twitter_route
            query += "OR h.`%s` < %s " % (self.twitter_route, actual_time)
            query += "WITH h LIMIT 1 "
            query += "SET h.`%s`=%s " % (self.twitter_route, distant_time)
            query += "RETURN h.token as token, "
            query += "h.secret as secret"
            with self.neo4j_driver.session() as db:
                results = db.run(query)
                user_token = results[0]["token"]
                user_secret = results[0]["secret"]
            if not user_token:
                print('All tokens are busy.... waiting.')
                time.sleep(10)
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
                response = self.api.request(self.RATE_LIMIT, {})
                break
            except Exception as exc:
                print("13 - ERROR -> %s. ConnectTimeout retrying in 10s..." % exc)
                time.sleep(10)
                continue
        parsed_response = json.loads(response.text)['resources']
        try:
            for category in parsed_response.keys():
                for route in parsed_response[category].keys():
                    if self.twitter_route in route:
                        reset_time = parsed_response[category][route]['reset']
            query = "MATCH (h:TOKEN{token:'%s'}) " % self.user_token
            query += "SET h.`%s`=%s" % (self.twitter_route, reset_time)
            self.__run_query(query)
        except Exception as exc:
            print("14 - ERROR -> %s. Failed to reset the token's timestamp." % exc)
            if 'errors' in parsed_response:
                e_code = parsed_response['errors'][0]['code']
                if e_code == 89:
                    query = "MATCH (h:TOKEN{token:'%s'}) " % self.user_token
                    query += "SET h:OLDTOKEN"
                    self.__run_query(query)
                    print("Error corrected, token deleted!")
                else:
                    print("Unknown error in __update_reset_time() function")
            else:
                print(
                    "Unknown error in _update_reset_time(). parsed_response = %s" % parsed_response)

    def __cleanup_last_session(self):
        """
        Remove properties for initialized twitter_route
        """
        query = "MATCH (h:TOKEN) "
        query += "WHERE EXISTS(h.`%s`) " % self.twitter_route
        query += "REMOVE h.`%s`" % self.twitter_route
        self.__run_query(query)

    def __run_query(self, query: str):
        print("running query: %s" % query)
        with self.neo4j_driver.session() as db:
            while True:
                try:
                    db.run(query)
                    break
                except Exception as exc:
                    e_name = type(exc).__name__
                    if e_name == "TransientError":
                        print(
                            "8 - ERROR -> %s. DB data is locked. Retrying..." % e_name)
                        print(str(exc))
                        time.sleep(2)
                        continue
                    elif e_name == "AddressError":
                        self.neo4j_driver = self.__connect_to_db()
                    else:
                        print("9 - UNKNOWN ERROR -> %s." % e_name)
                        time.sleep(2)
                        continue
