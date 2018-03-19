import queue
import random
import multiprocessing
from neo4j.v1 import GraphDatabase, basic_auth
import time
import os

from twitter import TwitterCrawler

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



def get_priority_users(batch_size):
    query = "MATCH (a:USER:QUEUED)"
    query += "WITH a "
    query += "ORDER BY a.timestamp ASC "
    query += "LIMIT %s " % batch_size
    query += "RETURN a.uid as uid"

    with driver.session() as db:
        results = db.run(query)
        user_list = []
        for user in results:
            user_list.append(user['uid'])
        return user_list   

queue_size = 100
batch_size = 200
num_crawlers = 1

q = multiprocessing.Queue(queue_size)
lock = multiprocessing.Lock()

for i in range(num_crawlers):
    crawler = multiprocessing.Process(target=TwitterCrawler, args=(q, lock))
    crawler.daemon = True
    crawler.start()

for unfinished in get_unfinished_list():
    q.put(unfinished)

while True:
    if q.empty():
        print("Searching priority users...")
        for user in get_priority_users(batch_size):
            q.put(user)
    time.sleep(2)
