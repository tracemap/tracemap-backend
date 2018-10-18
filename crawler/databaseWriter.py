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
                self.__log_to_file(self.name + " connected to the database.\n\n\n")
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
                    self.__log_to_file("Writing queue empty. Waiting...\n\n\n")
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
            self.__write_followers(user_id)
        elif file_name[-10:-4] == 'delete':
            self.__delete_user(user_id)
        elif file_name[-8:-4] == 'skip':
            self.__skip_user(user_id)

    def __request_followers(self, user_id, batch_number, batch_size):
        query = "MATCH (u:USER:QUEUED{uid:'%s'})" % user_id
        query += "<-[:FOLLOWS]-(f:USER) "
        query += "RETURN f.uid AS followerid "
        query += "SKIP %s " % (batch_number*batch_size)
        query += "LIMIT %s" % batch_size
        return [d['followerid'] for d in self.__run_get_query(query).data()]

    def __write_followers(self, user_id):
        num_followers = 0
        batch = 0
        batch_size = 1000000
        self.__log_to_file("Requesting followers...")
        followers_in_db = []
        next_batch = self.__request_followers(user_id, batch, batch_size)
        while len(next_batch) > 0:
            t0 = time.time()
            followers_in_db.extend(next_batch)
            batch += 1
            next_batch = self.__request_followers(user_id, batch, batch_size)
            self.__log_to_file("Time to complete batch %s: %s seconds." % (batch, time.time() - t0))
        self.__log_to_file("Followers requested successfully! List has %s followers." % len(followers_in_db))
        list_len = len(followers_in_db)
        if list_len > 0:
            with open("temp/%s_save.txt" % user_id, "r") as followers_file:
                followers_in_db = set(followers_in_db)
                set_len = len(followers_in_db)
                self.__log_to_file("List converted successfully into set! Set has %s followers." % set_len)
                if list_len != set_len:
                    self.__log_to_file("WARNING!!!WARNING!!!WARNING!!!WARNING!!!WARNING!!!WARNING!!!\n" +
                                       "      OLD FOLLOWERS LIST AND SET GOT DIFFERENT LENGTHS\n" +
                                       "WARNING!!!WARNING!!!WARNING!!!WARNING!!!WARNING!!!WARNING!!!")
                for line in followers_file.readlines():
                    new_followers = [f for f in line.replace('\n', '').split(',') if f not in followers_in_db]
                    old_followers = [f for f in line.replace('\n', '').split(',') if f in followers_in_db]
                    self.__log_to_file("Removing %s followers from set" % len(old_followers))
                    for old in old_followers:
                        followers_in_db.remove(old)
                    self.__log_to_file("Set has, now, %s users" % len(followers_in_db))
                    batch_num_followers = len(new_followers)
                    if batch_num_followers > 0:
                        self.__save_user_followers(user_id, new_followers)
                    num_followers += batch_num_followers
                self.__delete_relations(user_id, list(followers_in_db))
                self.__log_to_file("Old relations deleted: %s" % len(followers_in_db))
        else:
            self.__log_to_file("No user was in the database yet...")
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
        query += "UNWIND followers AS fid "
        query += "MATCH (u:USER:QUEUED{uid:'%s'}) " % user_id
        query += "MERGE (f:USER{uid: fid}) "
        query += "ON CREATE SET f.timestamp=%s, f:PRIORITY2 " % present_time
        query += "MERGE (u)<-[:FOLLOWS]-(f) "
        self.__run_query(query)
        self.__log_to_file("Followers of user %s saved: %s" % (user_id, len(followers)))

    def __update_user(self, user_id, num_followers):
        # Some verbosity about the kind of db write
        timestamp = math.floor(time.time())
        query = "MATCH (a:USER:QUEUED{uid:'%s'}) " % user_id
        query += "SET a.timestamp=%s, a:PRIORITY3 " % timestamp
        query += "REMOVE a:QUEUED"
        self.__run_query(query)
        self.__log_to_file("Updated user %s, label QUEUED erased -> num_followers: %s\n\n\n" % (user_id, num_followers))

    def __delete_user(self, user_id):
        # delete invalid user and connections
        query = "MATCH (u:USER:QUEUED{uid:'%s'}) " % user_id
        query += "DETACH DELETE u"
        self.__run_query(query)
        self.__log_to_file("Deleted user %s\n\n\n" % user_id)

    def __delete_relations(self, user_id, old_followers):
        self.__log_to_file("Deleting %s invalid relations from user %s." % (len(old_followers), user_id))
        rest = batch = old_followers
        batch_size = 50000
        while len(batch) > 0:
            batch, rest = rest[:batch_size], rest[batch_size:]
            query = "WITH %s AS oldfollowers " % batch
            query += "MATCH (u:QUEUED:USER{uid:'%s'})<-[r:FOLLOWS]-(f:USER) WHERE f.uid IN oldfollowers " % user_id
            query += "DELETE r"
            self.__run_query(query)
            self.__log_to_file("Batch of %s old followers deleted." % len(batch))

    def __skip_user(self, user_id):
        # skipping users having the wrong language
        # by setting their timestamp to a high number
        present_time = math.floor(time.time())
        future_time = present_time + (60 * 60 * 24 * 30 * 2)  # 2 months in the future
        query = "MATCH (u:USER:QUEUED{uid: '%s'}) " % user_id
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
                        self.__log_to_file(str(exc))
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
                        self.__log_to_file(str(exc))
                        time.sleep(2)
                        continue
            try:
                return result
            except Exception as exc:
                e_name = type(exc).__name__
                self.__log_to_file("11B - UNKNOWN ERROR -> %s. The value of result.data() is %s." % (e_name, result.data()))
                self.__log_to_file(str(exc))

    def __log_to_file(self, message):
        now = time.strftime("[%a, %d %b %Y %H:%M:%S] ", time.localtime())
        print(now + message)
        with open("log/"+self.name+".log", 'a') as log_file:
            log_file.write(now + message + '\n')
