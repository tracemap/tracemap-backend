from neo4j.v1 import GraphDatabase, basic_auth
import os
import time

while True:
    try:
        driver = GraphDatabase.driver(
            os.environ.get('NEO4J_URI'), auth=(
                os.environ.get('NEO4J_USER'),
                os.environ.get('NEO4J_PASSWORD')
                )
            )
        break
    except:
        print("Database not up.. retrying")
        time.sleep(5)
        continue

user_token = os.environ.get('USER_TOKEN')
user_secret = os.environ.get('USER_SECRET')

query = "MERGE (a:TOKEN{token:'%s', secret:'%s'}) " % (user_token, user_secret)
query += "ON CREATE SET a.timestamp=0"
with driver.session() as db:
    db.run(query)
print("user token parsed to database.")
