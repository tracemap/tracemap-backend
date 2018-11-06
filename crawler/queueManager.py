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
                __log_to_file("Resetting old BUSYTOKEN")
                db.run(token_query)
                __log_to_file("OLDBUSYTOKEN reset done.")
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
                __log_to_file("Getting unfinished users")
                results = db.run(query).data()[0]['user_ids']
                __log_to_file("%s unfinished users in the database." % len(results))
                break
            except Exception as exc:
                __log_to_file("ERROR -> %s. Getting unfinished users failed." % exc)
                time.sleep(3)
                continue
    user_list = []
    temp_folder_files = os.listdir("temp")
    temp_files = [tf for tf in temp_folder_files if "temp" in tf]
    for user_id in results:
        if ("temp_%s.txt" % user_id) in temp_files:
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
    # label priority1 users with the prio_ prefix
    if priority == 1:
        results = ["prio_%s" % user for user in results]
    user_list += results

    __log_to_file("Round of PRIORITY%s users, %s users so far in the batch." % (priority, len(user_list)))

    # if len(user_list) == queue_size or priority == 3:
    #     return user_list
    # else:
    #     priority += 1
    #     __log_to_file("Going to next priority, %s users remaining." % (queue_size - len(user_list)))
    #     return get_next_crawler_users(user_list, priority)
    return user_list

def get_next_writer_users():
    '''
    searches temp folder for user files and returns them sorted
    with returning prio_ prefixed users first after deleting their
    prefix.
    '''
    user_files = os.listdir("temp")
    # get big users 
    big_users = [uf for uf in user_files if "big_" in uf and uf not in last_write_q]
    big_users.sort(key = lambda x: os.path.getmtime("temp/" + x))
    big_users = big_users[:write_queue_size]
    # get not_prio users if not enough prio users
    nonprio_users = []
    if len(big_users) < write_queue_size:
        nonprio_users = [uf for uf in user_files if uf[0].isdigit() and uf not in last_write_q]
        nonprio_users.sort(key = lambda x: os.path.getmtime("temp/" + x))
    next_users = big_users + nonprio_users
    return next_users[:write_queue_size]

def get_next_prio_writer_users():
    '''
    searches temp folder for prio_ user files and returns
    the oldest of them.
    '''
    user_files = os.listdir("temp")
    # get prio users 
    prio_users = [uf for uf in user_files if "prio_" in uf and uf not in last_prio_write_q]
    prio_users.sort(key = lambda x: os.path.getmtime("temp/" + x))
    prio_users = prio_users[:prio_write_queue_size]
    return prio_users
    


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
    prio_write_queue_size = 200
    num_crawlers = 20
    num_writers = 1
    num_prio_writers = 1

    last_q = []
    last_write_q = []
    last_prio_write_q = []

    driver = __connect_to_db()

    lock = multiprocessing.Lock()
    q = multiprocessing.Queue(queue_size)
    write_q = multiprocessing.Queue(write_queue_size)
    prio_write_q = multiprocessing.Queue(prio_write_queue_size)

    __log_to_file("Queues initialized!")

    for i in range(num_crawlers):
        crawler = multiprocessing.Process(target=Crawler, args=(q, "crawler%s" % i))
        crawler.daemon = True
        crawler.start()
        __log_to_file("crawler%s" % i + " started!")

    for j in range(num_writers):
        writer = multiprocessing.Process(target=Writer, args=(write_q, lock, "writer%s" % j))
        writer.daemon = True
        writer.start()
        __log_to_file("Writer started!")

    for k in range(num_prio_writers):
        prio_writer = multiprocessing.Process(target=Writer, args=(prio_write_q, lock, "prio_writer%s" % j))
        prio_writer.daemon = True
        prio_writer.start()
        __log_to_file("Prio writer started!")

    for temp_file in os.listdir("temp"):
        if temp_file[:5] == "temp_":
            os.remove("temp/"+temp_file)

    # get unfinished users and add them to the queue
    uncrawled_list = get_unfinished_users()
    for uncrawled in uncrawled_list:
        q.put(uncrawled)

    while True:
        if q.qsize() < 20:
            __log_to_file("Filling crawler queue.")
            next_crawler_batch = get_next_crawler_users([])
            for user in next_crawler_batch:
                if user not in last_q:
                    try:
                        last_q.append(user)
                        q.put_nowait(user)
                    except:
                        break
            last_q = last_q[-queue_size:]

        if prio_write_q.qsize() < 20:
            next_prio_writer_batch = get_next_prio_writer_users()
            for user in next_prio_writer_batch:
                if user not in last_prio_write_q:
                    try:
                        last_prio_write_q.append(user)
                        prio_write_q.put_nowait(user)
                    except:
                        break
            last_prio_write_q = last_prio_write_q[-prio_write_queue_size:]
        if write_q.qsize() < 20:
            __log_to_file("Filling writer queue")
            next_writer_batch = get_next_writer_users()
            for user in next_writer_batch:
                if user not in last_write_q:
                    try:
                        last_write_q.append(user)
                        write_q.put_nowait(user)
                    except:
                        break
            # keep last_write_q until its as big as the write_queue_size
            # if exchanged with this_write_q at each iteration
            # the writer is sometimes still writing an old user which is
            # added again at the second iteration and causes errors
            last_write_q = last_write_q[-write_queue_size:]
        time.sleep(10)

