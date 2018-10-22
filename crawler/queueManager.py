import multiprocessing
from neo4j.v1 import GraphDatabase
import time
import os

from twitterCrawlers import Crawler
from databaseWriter import Writer


def get_unfinished_users():
    """
    Get all unfinished changes from the previous session.
    Reset :BUSYTOKEN labels to :TOKEN.
    Return users which already have a :QUEUED label.
    """

    # reset token used by the previous session
    token_query = "MATCH (h:BUSYTOKEN) "
    token_query += "SET h:TOKEN REMOVE h:BUSYTOKEN"

    while True:
        with driver.session() as db:
            try:
                db.run(token_query)
                break
            except Exception as exc:
                __log_to_file("ERROR -> %s. Resetting tokens failed." % exc)
                time.sleep(3)
                continue

    # request all database users having a :QUEUED label.
    query = "MATCH (a:QUEUED) "
    query += "RETURN COLLECT(a.uid) as user_ids"

    while True:
        with driver.session() as db:
            try:
                results = db.run(query).data()[0]['user_ids']
                break
            except Exception as exc:
                __log_to_file("ERROR -> %s. Getting unfinished users failed." % exc)
                time.sleep(3)
                continue
    user_list = []
    for user_id in results:
        if ("temp_%s.txt" % user_id) in os.listdir("temp"):
            user_list.append(user_id)
    __log_to_file("Adding %s old users to the crawler queue." % len(user_list))
    return user_list


def get_next_crawler_users(user_list, priority=1):
    """
    Get the next batch of users to refill the crawler queue.
    """
    remaining_users = queue_size - len(user_list)

    query = "MATCH (a:PRIORITY%s) " % priority
    query += "REMOVE a:PRIORITY%s " % priority
    query += "SET a:QUEUED "
    query += "WITH a.uid as uids LIMIT %s " % remaining_users
    query += "RETURN COLLECT(uids) as users "

    while True:
        with driver.session() as db:
            try:
                results = db.run(query).data()[0]['users']
                break
            except Exception as exc:
                __log_to_file("ERROR -> %s. Getting priority users failed." % exc)
                time.sleep(3)
                continue

    for user_id in results:
        user_list.append(user_id)

    __log_to_file("Round of PRIORITY%s users, %s users so far in the batch." % (priority, len(user_list)))

    if len(user_list) == queue_size or priority == 3:
        return user_list
    else:
        priority += 1
        __log_to_file("Going to next priority, %s users remaining." % (queue_size - len(user_list)))
        return get_next_crawler_users(user_list, priority)

def get_next_writer_users():
    user_files = os.listdir("temp")
    filter(lambda x: "temp" not in x, user_files)
    user_files.sort(key = lambda x: os.path.getmtime("temp/" + x))
    return user_files[:write_queue_size]
    


def __log_to_file(message):
    now = time.strftime("[%a, %d %b %Y %H:%M:%S] ", time.localtime())
    with open("log/queue_manager.log", 'a') as log_file:
        log_file.write(now + message + '\n')
    print(now + message + '\n')


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
            __log_to_file("Exception: %s -> Database not up or wrong credentials! retrying in 5s..." % exc)
            time.sleep(5)
            continue


if __name__ == '__main__':

    for log_file in os.listdir("log"):
        os.remove("log/"+log_file)
    __log_to_file("All log files removed.")

    queue_size = 200
    write_queue_size = 200
    num_crawlers = 20
    num_writers = 1

    driver = __connect_to_db()

    q = multiprocessing.Queue(queue_size)
    write_q = multiprocessing.Queue(write_queue_size)

    __log_to_file("Queues initialized!")

    for i in range(num_crawlers):
        crawler = multiprocessing.Process(target=Crawler, args=(q, "crawler%s" % i))
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

    # get unfinished users and add them to the queue
    uncrawled_list = get_unfinished_users()
    for uncrawled in uncrawled_list:
        q.put(uncrawled)

    __log_to_file("Finished putting all %s uncrawled users in the queue!" % len(uncrawled_list))

    while True:
        if q.empty():
            __log_to_file("Filling crawler queue.")
            for user in get_next_crawler_users([]):
                q.put_nowait(user)
        if write_q.empty():
            __log_to_file("Filling writer queue")
            for user in get_next_writer_users():
                write_q.put_nowait(user)
        time.sleep(10)

