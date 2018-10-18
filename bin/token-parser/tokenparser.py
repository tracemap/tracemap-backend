from neo4j.v1 import GraphDatabase, basic_auth
import json
import pymysql
import os

def create_token():
    return "TODO"

def get_tokens():
    tokens = [];
    conn = pymysql.connect(
            host=os.environ.get('MYSQL_HOST'), 
            user=os.environ.get('MYSQL_USER'), 
            passwd=os.environ.get('MYSQL_PASSWORD'), 
            db=os.environ.get('MYSQL_TOKENDB'))
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT accessToken, accessTokenSecret, userName FROM userCredentials")
    for response in cur:
        if response['accessTokenSecret']:
            tokens.append(response)
    return tokens

neo = GraphDatabase.driver(
    os.environ.get('NEO4J_URI'), auth=(
        os.environ.get('NEO4J_USER'),
        os.environ.get('NEO4J_PASSWORD')
    )
)

db = neo.session()

users = get_tokens()

for user in users:
    name = user['userName']
    token = user['accessToken']
    secret = user['accessTokenSecret']
    with db.begin_transaction() as tx:
        tx.run("MERGE (a:TOKEN{token:'%s', secret:'%s'}) SET a.user='%s', a.timestamp=0" % (token, secret, name) )


