from neo4j.v1 import GraphDatabase, basic_auth
import os

driver = GraphDatabase.driver(
    os.environ.get('NEO4J_URI'), auth=(
        os.environ.get('NEO4J_USER'),
        os.environ.get('NEO4J_PASSWORD')
        )
    )

def add_timestamp_uncrawled():
    query = "MATCH (a:USER) "
    query += "WHERE NOT EXISTS(a.timestamp) "
    query += "WITH a LIMIT 1000000 "
    query += "SET a.timestamp=2 "
    query += "RETURN COUNT(*)"
    with driver.session() as db:
        full_count = 0
        while True:
            result = db.run(query)
            count = result.single()[0]
            print("ADDED %s timestamps" % count)
            full_count += count
            if count == 0:
                print("ADDED %s timestamps in total." % full_count)
                break
            continue

def add_timestamp_crawled():
    query = "MATCH (u:USER)<-[r:FOLLOWS] "
    query += "WHERE u.timestamp=2 "
    query += "WITH u LIMIT 10000 "
    query += "SET u.timestamp=3 "
    query += "RETURN COUNT(*)"
    with driver.session() as db:
        full_count = 0
        while True:
            result = db.run(query)
            count = result.single()[0]
            print( "ADDED %s timestamps" % count)
            full_count += count
            if count == 0:
                print( "ADDED %s timestamps in total." % full_count)
                break
            continue

add_timestamp_uncrawled()
#add_timestamp_crawled()
