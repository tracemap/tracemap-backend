from neo4j.v1 import GraphDatabase
import os
import time
import math


class Writer:

    def __init__(self, write_q, name):
        self.name = name
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
            self.__delete_old_relations(user_id)
            self.__write_followers(user_id)
        elif file_name[-10:-4] == 'delete':
            self.__delete_user(user_id)

    def __request_followers(self, user_id, batch_number, batch_size):
        query = "MATCH (u:USER:QUEUED{uid:'%s'})" % user_id
        query += "<-[:FOLLOWS]-(f:USER) "
        query += "RETURN f.uid AS followerid "
        query += "SKIP %s " % (batch_number*batch_size)
        query += "LIMIT %s" % batch_size
        return [d['followerid'] for d in self.__run_get_query(query).data()]

    def __write_followers(self, user_id):
        num_new_followers = 0
        batch_size = 50000
        self.__log_to_file("Writing new followers for user %s." % user_id)
        with open("temp/%s_save.txt" % user_id, "r") as followers_file:
            batch = []
            for line in followers_file.readlines():
                batch.extend(line.replace('\n', '').split(','))
                if len(batch) >= batch_size:
                    self.__save_user_followers(user_id, batch)
                    num_new_followers += len(batch)
                    batch = []
            if len(batch) > 0:
                self.__save_user_followers(user_id, batch)
                num_new_followers += len(batch)
        self.__update_user(user_id, num_new_followers)

    def __save_user_followers(self, user_id, followers):
        # Add a batch of followers to the db
        present_time = math.floor(time.time())
        query = "MATCH (u:QUEUED:USER{uid:'%s'}) " % user_id
        query += "WITH %s AS followers, u " % followers
        query += "UNWIND followers AS fid "
        query += "MERGE (f:USER{uid: fid}) "
        query += "ON CREATE SET f.timestamp=%s, f:PRIORITY2 " % present_time
        query += "CREATE (u)<-[:FOLLOWS]-(f)"
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

    def __delete_old_relations(self, user_id):
        self.__log_to_file("Deleting old relations from user %s." % user_id)
        batch_size = 100000
        while True:
            query = "MATCH (u:QUEUED:USER{uid:'%s'})<-[r:FOLLOWS]-() " % user_id
            query += "WITH r LIMIT %s " % batch_size
            query += "DELETE r "
            query += "RETURN COUNT(r)"
            data = self.__run_get_query(query).data()[0]['COUNT(r)']
            self.__log_to_file("%s relations deleted." % data)
            if data < batch_size:
                break
            else:
                continue

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
