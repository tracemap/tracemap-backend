from neo4j.v1 import GraphDatabase, basic_auth
import json
import time
import os

def request_database(request_string):
    uri = os.environ.get('NEO4J_URI')
    driver = GraphDatabase.driver(uri, auth=(os.environ.get('NEO4J_USER'),
                                  os.environ.get('NEO4J_PASSWORD')))
    with driver.session() as session:
        with session.begin_transaction() as transaction:
            return transaction.run(request_string).data()

def build_lookup_dictionary(node_list):
    result_dictionary = {}
    for node in node_list:
        result_dictionary.update({node.id:node.properties['uid']})
    return result_dictionary

def get_followers(user_ids):
    followers_json = {}
    database_query = ''
    first_iteration = True
    if len(user_ids) <= 1:
        return followers_json
    for uid in user_ids:
        if first_iteration:
            database_query += 'MATCH (u:USER) WHERE u.uid = "' + uid + '" '
            first_iteration = False
            continue
        database_query += 'OR u.uid = "' + uid + '" '
    database_query += 'WITH COLLECT(u) AS us UNWIND us AS u1 UNWIND us AS u2 '
    database_query += 'MATCH (u1)-[r]->(u2) RETURN COLLECT(r), us;'
    database_response = request_database(database_query)
    dictionary_lookup = build_lookup_dictionary(database_response[0]['us'])
    for relation in database_response[0]['COLLECT(r)']:
        user = dictionary_lookup[relation.end]
        follower = dictionary_lookup[relation.start]
        if user not in followers_json:
            followers_json.update({user:{'followers':[follower]}})
        else:
            followers_json[user]['followers'].append(follower)
    return followers_json

def add_user_info(user_info):
    pass

def get_user_info(user_id):
    pass
