import multiprocessing
from neo4j.v1 import GraphDatabase
import time
import os

from twitterCrawlers import Crawler
from databaseWriter import Writer


def get_uncrawled_list():

    token_query = "MATCH (h:BUSYTOKEN) "
    token_query += "SET h:TOKEN REMOVE h:BUSYTOKEN"

    while True:
        with driver.session() as db:
            try:
                db.run(token_query)
                break
            except Exception as exc:
                __log_to_file("ERROR -> %s. Resetting tokens failed." % exc)
                continue

    query = "MATCH (a:QUEUED) "
    query += "RETURN a.uid as uid"

    while True:
        with driver.session() as db:
            try:
                results = db.run(query)
                break
            except Exception as exc:
                __log_to_file("ERROR -> %s. Getting unfinished users failed." % exc)
                continue

    user_list = []
    for unfinished_user in results:
        uid = unfinished_user['uid']
        pre_saved = '%s_save.txt' % uid in os.listdir("temp")
        pre_skipped = '%s_skip.txt' % uid in os.listdir("temp")
        pre_deleted = '%s_delete.txt' % uid in os.listdir("temp")
        if not (pre_saved or pre_skipped or pre_deleted):
            user_list.append(uid)
    return user_list


# this function is checking for users from the highest
# priority (1) to the lowest (3) and adds them to the batch
# until it is filled.
def get_priority_users(user_list, priority=1):

    remaining_users = batch_size - len(user_list)

    query = "MATCH (a:PRIORITY%s) " % priority
    query += "REMOVE a:PRIORITY%s " % priority
    query += "SET a:QUEUED "
    query += "RETURN a.uid as uid "
    query += "LIMIT %s" % remaining_users

    while True:
        with driver.session() as db:
            try:
                results = db.run(query).data()
                break
            except Exception as exc:
                __log_to_file("ERROR -> %s. Getting priority users failed." % exc)
                continue

    for priority_user in results:
        user_list.append(priority_user['uid'])

    __log_to_file("Round of PRIORITY%s users, %s users so far in the batch." % (priority, len(user_list)))

    if len(user_list) == batch_size or priority == 3:
        return user_list
    else:
        priority += 1
        __log_to_file("Going to next priority, %s users remaining." % (batch_size - len(user_list)))
        return get_priority_users(user_list, priority)


def __log_to_file(message):
    now = time.strftime("[%a, %d %b %Y %H:%M:%S] ", time.localtime())
    with open("log/queue_manager.log", 'a') as log_file:
        log_file.write(now + message + '\n')


def __connect_to_db():
    while True:
        try:
            driver = GraphDatabase.driver(
                os.environ.get('NEO4J_URI'), auth=(
                    os.environ.get('NEO4J_USER'),
                    os.environ.get('NEO4J_PASSWORD')
                )
            )
            __log_to_file("Connected to the database.")
            return driver
        except Exception as exc:
            self.__log_to_file("Exception: %s -> Database not up or wrong credentials! retrying in 5s..." % exc)
            time.sleep(5)
            continue


if __name__ == '__main__':

    for log_file in os.listdir("log"):
        os.remove("log/"+log_file)
    __log_to_file("All log files removed.")

    queue_size = 100
    write_queue_size = 100
    batch_size = 500
    num_crawlers = 20
    num_writers = 1

    driver = __connect_to_db()

    q = multiprocessing.Queue(queue_size)
    write_q = multiprocessing.Queue(write_queue_size)

    __log_to_file("Queues set!")

    for i in range(num_crawlers):
        crawler = multiprocessing.Process(target=Crawler, args=(q, write_q, "crawler%s" % i))
        crawler.daemon = True
        crawler.start()
        __log_to_file("crawler%s" % i + " started!")

    for j in range(num_writers):
        writer = multiprocessing.Process(target=Writer, args=(write_q, "writer%s" % j))
        writer.daemon = True
        writer.start()
        __log_to_file("Writer started!")

    for temp_file in os.listdir("temp"):
        if temp_file[:5] == "temp_":
            os.remove("temp/"+temp_file)
            continue
        write_q.put("temp/"+temp_file)

    __log_to_file("Finished putting all %s crawled users in the write_queue!" % len(os.listdir("temp")))

    uncrawled_list = get_uncrawled_list()

    for uncrawled in uncrawled_list:
        q.put(uncrawled)

    __log_to_file("Finished putting all %s uncrawled users in the queue!" % len(uncrawled_list))

    empty_state = True if q.empty() else False

    while True:
        if not q.empty():
            if empty_state:
                __log_to_file("Queue not yet empty. Sleeping....\n")
                empty_state = False
            time.sleep(30)
            continue
        empty_state = True
        __log_to_file("Getting new batch...")
        for user in get_priority_users([]):
            # labels = get_user_labels(user)
            # if 'QUEUED' not in labels:
            #     __log_to_file("QUEUED label missing in user %s (%s)" % (user, labels))
            #     continue
            q.put(user)

