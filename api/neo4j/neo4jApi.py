from neo4j.v1 import GraphDatabase, basic_auth
import json
import time
import os

"""This function accesses the database with a query 'request_string' """
def __request_database(request_string):
    uri = os.environ.get('NEO4J_URI')
    driver = GraphDatabase.driver(uri, auth=(os.environ.get('NEO4J_USER'),
                                  os.environ.get('NEO4J_PASSWORD')))
    with driver.session() as session:
        with session.begin_transaction() as transaction:
            return transaction.run(request_string).data()

"""This function creates a lookup map between Twitter uids and neo4j node ids"""
def __build_lookup_dictionary(node_list):
    result_dictionary = {}
    for node in node_list:
        result_dictionary.update({node.id:node.properties['uid']})
    return result_dictionary

"""This function formats a property dictionary to insert it in a Cypher query"""
def __format_property_string(property_dictionary):
    if property_dictionary == {}:
        return '{}'
    property_string = '{'
    for key in property_dictionary.keys():
        value = property_dictionary[key]
        property_string += key + ': '
        if type(value) is str:
            property_string += '"' + value + '", '
        else:
            property_string += str(value) + ', '
    property_string = property_string[:-2]
    property_string += '}'
    return property_string

"""This function gets all relations in the database between a set of users"""
def get_followers(user_ids):
    followers_dictionary = {}
    database_query = ''
    first_iteration = True
    if len(user_ids) <= 1:
        return followers_dictionary
    for uid in user_ids:
        if first_iteration:
            database_query += 'MATCH (u:USER) WHERE u.uid = "' + uid + '" '
            first_iteration = False
            continue
        database_query += 'OR u.uid = "' + uid + '" '
    database_query += 'WITH COLLECT(u) AS us UNWIND us AS u1 UNWIND us AS u2 '
    database_query += 'MATCH (u1)-[r]->(u2) RETURN COLLECT(r), us;'
    database_response = __request_database(database_query)
    if database_response == []:
        return followers_dictionary
    dictionary_lookup = __build_lookup_dictionary(database_response[0]['us'])
    for relation in database_response[0]['COLLECT(r)']:
        user = dictionary_lookup[relation.end]
        follower = dictionary_lookup[relation.start]
        if user not in followers_dictionary:
            followers_dictionary.update({user:[follower]})
        else:
            followers_dictionary[user].append(follower)
    return {'response': followers_dictionary}

"""This function does not create new nodes, users must be in database already"""
def add_user_info(user_info):
    if 'response' not in user_info.keys():
        """The user_info dictionary does not contain the 'response' key..."""
        return False
    success = True
    for user_id in user_info['response'].keys():
        database_query = 'MATCH (user:USER {uid: "'+user_id+'"}) ' +\
            'SET user += ' +\
            __format_property_string(user_info['response'][user_id]) + ' ' +\
            'RETURN user'
        database_response = __request_database(database_query)
        if database_response == []:
            """So far, it does not check if info was written correctly"""
            success = False
    return success

"""This function gets info of ONE user per time from the database"""
def get_user_info(user_id):
    database_query = 'MATCH (user:USER {uid: "'+user_id+'"}) ' +\
        'RETURN user'
    database_response = __request_database(database_query)
    """Just to be on the safe side"""
    if len(database_response) == 0:
        return {}
    return {'response':{user_id:database_response[0]['user'].properties}}


def get_crawled_users(user_ids):
    user_list = []
    if len(user_ids) < 1:
        return user_list
    for user_id in user_ids:
        database_query = "MATCH (a:USER{uid:'" + user_id + "'}) " + "OPTIONAL MATCH (a)<-[:FOLLOWS]-(b) " + "RETURN count(b)"
        number_followers = __request_database(database_query)[0]['count(b)']
        if number_followers > 0:
            user_list.append(user_id)
    return user_list
