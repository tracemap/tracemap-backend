import queue
import random
import multiprocessing
from neo4j.v1 import GraphDatabase, basic_auth
import time
import os

from twitter import TwitterCrawler


queue_size = 100
batch_size = 200
num_crawlers = 1

q = multiprocessing.Queue(queue_size)
lock = multiprocessing.Lock()

while True:
    try:
        driver = GraphDatabase.driver(
            os.environ.get('NEO4J_URI'), auth=(
                os.environ.get('NEO4J_USER'),
                os.environ.get('NEO4J_PASSWORD')
                )
            )
        print("Connected to the database.")
        break
    except:
        print("Database not up or wrong credentials.. retrying...")
        time.sleep(5)
        continue

def get_unfinished_list():
    query = "MATCH (a:USER:QUEUED) "
    query += "RETURN a.uid as uid"
    with driver.session() as db:
        results = db.run(query)
        user_list = []
        for user in results:
            user_list.append(user['uid'])
        return user_list


# this function is checking for users from the highest
# priority (1) to the lowest (3) and adds them to the batch
# until its filled.
def get_priority_users(user_list=[], priority=1):
    remaining_users = batch_size - len(user_list)
    query = "MATCH (a:USER:PRIORITY%s) " % priority
    query += "REMOVE a:PRIORITY%s " % priority
    query += "SET a:QUEUED "
    query += "RETURN a.uid as uid "
    query += "LIMIT %s" % remaining_users

    
    with driver.session() as db:
        results = db.run(query)
    for user in results:
        user_list.append(user['uid'])
    if len(user_list) == batch_size or priority == 3:
        return user_list
    else:
        priority += 1
        return get_priority_users(user_list, priority)



for i in range(num_crawlers):
    crawler = multiprocessing.Process(target=TwitterCrawler, args=(q, lock))
    crawler.daemon = True
    crawler.start()

for unfinished in get_unfinished_list():
    q.put(unfinished)

while True:
    # if statement was necessary because searching and 
    # sorting timestamps blocked the whole crawling process
    # so it was more secure to wait until the crawlers are finished
    if q.empty():
        print("Searching priority users...")
        for user in get_priority_users():
            q.put(user)
    time.sleep(2)
