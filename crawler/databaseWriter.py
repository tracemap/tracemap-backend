from neo4j.v1 import GraphDatabase
import os
import time
import math


class Writer:

    def __init__(self, write_q, lock, name):
        self.name = name
        self.__log_to_file(self.name + " is initialized.")

        self.driver = self.__connect_to_db()

        self.write_q = write_q

        self.lock = lock

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
        self.prio = False
        self.locked = False
        
        while True:
            if self.write_q.empty():
                if self.prio == True and self.locked == True:
                    self.locked = False
                    self.lock.release()
                if empty_state:
                    self.__log_to_file("Writing queue empty. Waiting...\n\n\n")
                    empty_state = True
                time.sleep(10)
                continue
            empty_state = False
            if "prio" in self.name:
                self.prio = True
            if self.prio == True and self.locked == False:
                self.locked = True
                self.lock.acquire()
            file_name = self.write_q.get()
            self.db_write(file_name)
            os.remove("temp/" + file_name)

    def db_write(self, file_name):
        user_id = file_name
        if "big_" in file_name:
            user_id = file_name[4:]
        elif "prio_" in file_name:
            user_id = file_name[5:] 
        user_id = user_id.split('_')[0]
        if file_name[-8:-4] == 'save':
            self.__log_to_file("WRITING: %s" % user_id)
            self.__delete_old_relations(user_id)
            self.__write_followers(user_id, file_name)
        elif file_name[-10:-4] == 'delete':
            self.__delete_user(user_id)

    def __write_followers(self, user_id, file_name):
        num_new_followers = 0
        batch_size = 50000
        self.__log_to_file("Writing new followers for user %s." % user_id)
        with open("temp/%s" % file_name, "r") as followers_file:
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
                    if self.prio == False:
                        # if non prio user, check and wait until prio lock
                        # is not set and set lock for this batch
                        while True:
                            no_prio_lock = self.lock.acquire(False)
                            if no_prio_lock == True:
                                break
                            time.sleep(1)
                        db.run(query)
                        self.lock.release()
                    else:
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
                    if self.prio == False:
                        # if non prio user, check and wait until prio lock
                        # is not set and set lock for this batch
                        while True:
                            no_prio_lock = self.lock.acquire(False)
                            if no_prio_lock == True:
                                break
                            time.sleep(1)
                        result = db.run(query)
                        self.lock.release()
                    else:
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
        print("%s@%s: %s" % (now, self.name, message))
        with open("log/"+self.name+".log", 'a') as log_file:
            log_file.write(now + message + '\n')
