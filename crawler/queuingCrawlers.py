import queue
from threading import Thread
import random
from neo4j.v1 import GraphDatabase, basic_auth
import time

t0 = time.time()

uri = os.environ.get('neo4j_url')
driver = GraphDatabase.driver(uri, auth=(os.environ.get('neo4j_username'),
                                         os.environ.get('neo4j_password')))

def get_unfinished_list():
    with driver.session() as session:
        with session.begin_transaction() as tx:
            response = tx.run('MATCH (a:USER:UsersQueue) ' +
                              'RETURN a').data()
            users_list = [(0,
                           node['a'].properties['uid']) for node in response]
            return users_list

def get_priority_users(batch_size):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            response = tx.run('MATCH (a:USER) WHERE NOT a:UsersQueue ' +
                              'RETURN a ORDER BY a.priority ' +
                              'LIMIT ' + str(batch_size)).data()
            users_list = [(node['a'].properties['priority'],
                           node['a'].properties['uid']) for node in response]
            for user in users_list:
                tx.run('MATCH (a:USER{uid:"'+user[1]+'"}) SET a:UsersQueue')
            return users_list

def remove_label(uid):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            tx.run('MATCH (a:USER:UsersQueue{uid:"'+uid+'"}) ' +
                   'REMOVE a:UsersQueue')   

def crawl(user):
    """get token, API Request, get info, update database ->
    delete node if it doesn't exist anymore, 
    delete old relations, create new relations,
    check for errors, update 'priority' property of the node in the DB
    with time.time() if crawl was successful, otherwise leave it as is."""
    time.sleep(10*random.random())


def crawler(q):
    global threads
    while True:
        user = q.get()
        crawl(user[1])
        remove_label(user[1])
        q.task_done()


queue_size = 100
batch_size = 500
num_crawlers = 30

q = queue.PriorityQueue(queue_size)

for i in range(num_crawlers):
    Crawler = Thread(target=crawler, args=(q,))
    Crawler.daemon = True
    Crawler.start()

for unfinished in get_unfinished_list():
    q.put(unfinished)

while True:
    users = get_priority_users(batch_size)
    for user in users:
        q.put(user)

