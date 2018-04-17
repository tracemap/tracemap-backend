import multiprocessing
from neo4j.v1 import GraphDatabase
import time
import os

from twitter import TwitterCrawler


def get_unfinished_list():

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
        user_list.append(unfinished_user['uid'])
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


def get_user_labels(user):

    query = "MATCH (u:USER{uid:'%s'}) " % user
    query += "RETURN LABELS(u) AS labels"

    with driver.session() as db:
        results = db.run(query)

    try:
        return results.data()[0]['labels']
    except Exception as exc:
        return {}


def __log_to_file(message):
    print(message)
    now = time.strftime("[%a, %d %b %Y %H:%M:%S] ", time.localtime())
    with open("log/queue_manager.log", 'a') as log_file:
        log_file.write(now + message + '\n')


if __name__ == '__main__':

    queue_size = 100
    batch_size = 500
    num_crawlers = 20

    while True:
        try:
            driver = GraphDatabase.driver(
                os.environ.get('NEO4J_URI'), auth=(
                    os.environ.get('NEO4J_USER'),
                    os.environ.get('NEO4J_PASSWORD')
                )
            )
            __log_to_file("Connected to the database.")
            break
        except Exception as exc:
            __log_to_file("Exception: %s -> Database not up or wrong credentials! retrying in 5s..." % exc)
            time.sleep(5)
            continue

    q = multiprocessing.Queue(queue_size)
    __log_to_file("Queue set!")

    for i in range(num_crawlers):
        crawler = multiprocessing.Process(target=TwitterCrawler, args=(q, "crawler%s" % i))
        crawler.daemon = True
        crawler.start()
        __log_to_file("crawler%s" % i + " started!")

    unfinished_list = get_unfinished_list()
    __log_to_file("Unfinished_list contains %s users..." % len(unfinished_list))
    for unfinished in unfinished_list:
        q.put(unfinished)
    __log_to_file("Finished putting all unfinished users in the queue!")

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

