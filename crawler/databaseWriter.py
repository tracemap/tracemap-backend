from neo4j.v1 import GraphDatabase
import os
import time
import math


class Writer:

    def __init__(self, write_q, name):
        self.name = name
        self.languages = ["de", "en"]
        self.__log_to_file(self.name + " is initialized.")

        self.driver = self.__connect_to_db()

        self.write_q = write_q

        # Eliminating the use of locks for now
        # self.lock = lock;

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
        empty_state = True if self.write_q.empty() else False
        while True:
            if self.write_q.empty():
                if not empty_state:
                    self.__log_to_file("Writing queue empty. Waiting...")
                    empty_state = True
                time.sleep(10)
                continue
            empty_state = False
            file_name = self.write_q.get()
            self.db_write(file_name)
            os.remove(file_name)

    def db_write(self, file_name):
        user_id = file_name[5:].split('_')[0]
        if file_name[-8:-4] == 'save':
            self.__log_to_file("WRITING: %s" % user_id)
            self.__delete_connections(user_id)
            self.__write_followers(user_id)
        elif file_name[-10:-4] == 'delete':
            self.__delete_user(user_id)
        elif file_name[-8:-4] == 'skip':
            self.__skip_user(user_id)

    def __write_followers(self, user_id):
        num_followers = 0
        with open("temp/%s_save.txt" % user_id, "r") as followers_file:
            for line in followers_file.readlines():
                followers = line.replace('\n', '').split(',')
                batch_num_followers = len(followers)
                if batch_num_followers > 0:
                    self.__save_user_followers(user_id, followers)
                num_followers += batch_num_followers
        self.__update_user(user_id, num_followers)

    def __save_user_followers(self, user_id, followers):
        # Add a batch of followers to the db
        present_time = math.floor(time.time())
        query = "WITH %s AS followers " % followers
        query += "UNWIND followers AS follower "
        query += "MATCH (u:QUEUED{uid:'%s'}) " % user_id
        # query += "FOREACH (follower IN followers | "
        query += "MERGE (f:USER{uid: follower}) "
        query += "ON CREATE SET f.timestamp=%s, f:PRIORITY2 " % present_time
        query += "MERGE (u)<-[:FOLLOWS]-(f) "  #) "
        self.__run_query(query)
        self.__log_to_file("Followers of user %s saved" % user_id)

    def __update_user(self, user_id, num_followers):
        # Some verbosity about the kind of db write
        timestamp = math.floor(time.time())
        self.__log_to_file("Going to update...")
        query = "MATCH (a:QUEUED{uid:'%s'}) " % user_id
        query += "SET a.timestamp=%s, a:PRIORITY3 " % timestamp
        query += "REMOVE a:QUEUED"
        self.__run_query(query)
        self.__log_to_file("Updated user %s, label QUEUED erased -> num_followers: %s\n\n\n" % (user_id, num_followers))

    def __delete_user(self, user_id):
        # delete invalid user and connections
        query = "MATCH (u:QUEUED{uid:'%s'}) " % user_id
        query += "DETACH DELETE u"
        self.__run_query(query)
        self.__log_to_file("Deleted user %s\n\n\n" % user_id)

    def __delete_connections(self, user_id):
        while True:
            query = "MATCH (u:QUEUED{uid: '%s' })<-[r:FOLLOWS]-() " % user_id
            query += "WITH r LIMIT 100000 "
            query += "DELETE r "
            query += "RETURN count(*)"
            result = self.__run_get_query(query)
            if result == 0:
                break
        self.__log_to_file("Connections from user %s deleted" % user_id)

    def __skip_user(self, user_id):
        # skipping users having the wrong language
        # by setting their timestamp to a high number
        present_time = math.floor(time.time())
        future_time = present_time + (60 * 60 * 24 * 60)  # 2 months in the future
        query = "MATCH (u:QUEUED{uid: '%s'}) " % user_id
        query += "SET u.timestamp=%s, u:PRIORITY3 " % future_time
        query += "REMOVE u:QUEUED"
        self.__run_query(query)
        self.__log_to_file("Skipped user %s\n\n\n" % user_id)

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

    def __run_get_query(self, query):
        with self.driver.session() as db:
            while True:
                try:
                    result = db.run(query)
                    break
                except Exception as exc:
                    e_name = type(exc).__name__
                    if e_name == "TransientError":
                        self.__log_to_file("10 - ERROR -> %s. DB data is locked. Retrying..." % e_name)
                        self.__log_to_file(str(exc))
                        time.sleep(2)
                        continue
                    elif e_name == "AddressError":
                        self.driver = self.__connect_to_db()
                    else:
                        self.__log_to_file("11 - UNKNOWN ERROR -> %s." % e_name)
                        time.sleep(2)
                        continue
            try:
                return result.single()[0]
            except Exception as exc:
                self.__log_to_file("11B - UNKNOWN ERROR -> %s. The value of result.data() is %s." % (exc, result.data()))

    def __log_to_file(self, message):
        # print(message)
        now = time.strftime("[%a, %d %b %Y %H:%M:%S] ", time.localtime())
        with open("log/"+self.name+".log", 'a') as log_file:
            log_file.write(now + message + '\n')
