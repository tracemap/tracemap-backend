from neo4j import GraphDatabase, basic_auth
from classes.users import User, TmUser
from typing import List
import json
import time
import os
import math

class Neo4jApi:
    
    def __init__(self):
        uri = os.environ.get('NEO4J_URI')
        self.driver = GraphDatabase.driver(uri, auth=(
            os.environ.get('NEO4J_USER'),
            os.environ.get('NEO4J_PASSWORD')
            ))

    def __close(self):
        self.driver.close()

    def __request_database(self, cypher_statement: str, cypher_params: dict) -> list:
        """
        Request the database using and return the results data.  
        :param cypher_statement: the cypher query as a string  
        :param cypher_params: the query parameters as a dict  
        :returns: the returned data as a list of dictionaries 
        """
        with self.driver.session() as session:
            with session.begin_transaction() as transaction:
                return transaction.run(cypher_statement, cypher_params).data()

    def __build_lookup_dictionary(self, node_list) -> dict:
        """
        Creates a lookup map between Twitter uids and neo4j node ids.  
        :param node_list: list of neo4j node objects  
        :returns: a dictionary with tupels of node_ids and twitter user ids
        """
        result_dictionary = {}
        for node in node_list:
            result_dictionary.update({node.id:node.properties['uid']})
        return result_dictionary

    # """This function formats a property dictionary to insert it in a Cypher query"""
    # def __format_property_string(self, property_dictionary):
    #     if property_dictionary == {}:
    #         return '{}'
    #     property_string = '{'
    #     for key in property_dictionary.keys():
    #         value = property_dictionary[key]
    #         property_string += key + ': '
    #         if type(value) is str:
    #             property_string += '"' + value + '", '
    #         else:
    #             property_string += str(value) + ', '
    #     property_string = property_string[:-2]
    #     property_string += '}'
    #     return property_string

    def write_tweet(self, tweet_id: str) -> dict:
        """
        Create a new tweet node if it doesn't exist.  
        :param tweet_id: the tweet_id from twitter  
        """
        cypher_statement = "MERGE (tweet:Tweet{id:{id}})"
        cypher_params = {'id': tweet_id}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def write_user(self, user: User) -> dict:
        """
        Create a new user node if it doesn't exist.  
        :param user_id: the user_id from twitter  
        """
        cypher_statement =  "MERGE (user:User{id:{id}})"
        cypher_params = {'id': user.id}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def write_tm_user(
        self, 
        tmUser: TmUser) -> dict:
        """
        Create a new tracemap user or alter existing user to 
        tracemap user.  
        :param user_id: the user_id from twitter  
        :param twitter_token: the users oauth token from twitter  
        :param twitter_secret: the users oauth secret from twitter  
        :param session_token: the users session_token from browser storage
        """
        cypher_statement = """
        MERGE (user:User{id:{id}}) 
        SET user = {props} 
        SET user:TmUser
        """
        cypher_params = {'id': tmUser.id, 'props': tmUser.__dict__}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def write_retweeters(
        self,
        tweet_id: str,
        users: List[User]) -> dict:
        """
        Add a list of retweeters to the tweet node with tweet_id.  
        :param tweet_id: the tweets id  
        :param users: the list of User objects
        """
        for user in users:
            if not hasattr(user, 'time'):
                return {'success': False, 'error': f'Time missing for user {user.id}'}
        cypher_statement = """
        MERGE (tweet:Tweet{id:{tweet_id}})
        WITH tweet, {users} AS users 
        UNWIND users as user 
        MERGE (u:User{id:user.id}) 
        MERGE (tweet)<-[retweeted:RETWEETED]-(u) 
        SET retweeted.time = user.time
        """
        user_dicts = list(map(lambda x: x.__dict__, users))
        cypher_params = {'tweet_id': tweet_id, 'users': user_dicts}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def write_author(
        self,
        tweet_id: str,
        author: User) -> dict:
        """
        Connect a tweet with a user node through an author relation.  
        :param tweet_id: the tweets id  
        :param author: the author User object 
        """
        if not hasattr(author, 'time'):
            return {'success': False, 'error': f'Time missing for user {author.id}'}
        cypher_statement = """
        MERGE (tweet:Tweet{id:{tweet_id}}) 
        WITH tweet, {author} as author 
        MERGE (user:User{id:author.id}) 
        MERGE (tweet)<-[authored:AUTHORED]-(user) 
        SET authored.time = author.time 
        """
        cypher_params = {'tweet_id': tweet_id, 'author': author.__dict__}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def write_tweet_request(
        self,
        tweet_id: str,
        requester: TmUser) -> dict:
        """
        Connect a TmUser with a Tweet through a requested relation. 
        This marks the tweet for the crawlers to retrieve the followship 
        relations.
        """
        if not hasattr(requester, 'time'):
            return {'success': False, 'error': f'Time missing for user {requester.id}'}
        cypher_statement = """
        MERGE (tweet:Tweet{id:{tweet_id}}) 
        WITH tweet, {requester} as requester 
        MATCH (user:TmUser{id:requester.id}) 
        MERGE (tweet)<-[requested:REQUESTED]-(user) 
        SET requested.time = requester.time
        """
        cypher_params = {'tweet_id': tweet_id, 'requester': requester.__dict__}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def get_tm_user(
        self,
        user_id: str) -> TmUser:
        """
        Get the registered TmUser by id.  
        :param id: the users id
        """
        cypher_statement = """
        MATCH (u:TmUser) 
        RETURN properties(u) as data
        """
        cypher_params = {'user_id': user_id}
        result = self.__request_database(cypher_statement, cypher_params)
        if result == []:
            return None
        else:
            user_dict = result[0]['data']
            return TmUser(
                user_dict['id'], 
                user_dict['auth_token'], 
                user_dict['auth_secret'], 
                user_dict['session_token'])

# """This function gets all relations in the database between a set of users"""
# def get_followers(user_ids):
#     followers_dictionary = {}
#     database_query = ''
#     first_iteration = True
#     if len(user_ids) <= 1:
#         return followers_dictionary
#     for uid in user_ids:
#         if first_iteration:
#             database_query += 'MATCH (u:USER) WHERE u.uid = "' + uid + '" '
#             first_iteration = False
#             continue
#         database_query += 'OR u.uid = "' + uid + '" '
#     database_query += 'WITH COLLECT(u) AS us UNWIND us AS u1 UNWIND us AS u2 '
#     database_query += 'MATCH (u1)-[r]->(u2) RETURN COLLECT(r), us;'
#     database_response = self.__request_database(database_query)
#     if database_response == []:
#         return followers_dictionary
#     dictionary_lookup = self.__build_lookup_dictionary(database_response[0]['us'])
#     for relation in database_response[0]['COLLECT(r)']:
#         user = dictionary_lookup[relation.end]
#         follower = dictionary_lookup[relation.start]
#         if user not in followers_dictionary:
#             followers_dictionary.update({user:[follower]})
#         else:
#             followers_dictionary[user].append(follower)
#     return followers_dictionary


# """This function does not create new nodes, users must be in database already"""
# def add_user_info(user_info):
#     if 'response' not in user_info.keys():
#         """The user_info dictionary does not contain the 'response' key..."""
#         return False
#     success = True
#     for user_id in user_info['response'].keys():
#         database_query = 'MATCH (user:USER {uid: "'+user_id+'"}) ' +\
#             'SET user += ' +\
#             __format_property_string(user_info['response'][user_id]) + ' ' +\
#             'RETURN user'
#         database_response = __request_database(database_query)
#         if database_response == []:
#             """So far, it does not check if info was written correctly"""
#             success = False
#     return success


# """This function gets info of ONE user per time from the database"""
# def get_user_info(user_id):
#     database_query = 'MATCH (user:USER {uid: "'+user_id+'"}) ' +\
#         'RETURN user'
#     database_response = __request_database(database_query)
#     """Just to be on the safe side"""
#     if len(database_response) == 0:
#         return {}
#     return {user_id:database_response[0]['user'].properties}


# def label_unknown_users(user_ids):
#     time_now = math.floor(time.time())
#     three_month = 60 * 60 * 24 * 90

#     query = "WITH %s AS USERS " % user_ids
#     query += "FOREACH (U IN USERS | MERGE (X:USER{uid:U}) "
#     query += "FOREACH (ignoreMe in CASE WHEN (X:PRIORITY2 OR "
#     query += "(X:PRIORITY3 AND X.timestamp < %s) OR " % (time_now - three_month)
#     query += "LABELS(X)=['USER']) "
#     query += "THEN [1] ELSE [] END | "
#     query += "SET X:PRIORITY1 REMOVE X:PRIORITY2, X:PRIORITY3))"
#     __request_database(query)

#     query2 = "WITH %s AS USERS " % user_ids
#     query2 += "MATCH (u:PRIORITY1) "
#     query2 += "WHERE u.uid IN USERS "
#     query2 += "RETURN COLLECT(u.uid) as uncrawled"
#     uncrawled = __request_database(query2)[0]["uncrawled"]

#     query2 = "WITH %s AS USERS " % user_ids
#     query2 += "MATCH (u:QUEUED) "
#     query2 += "WHERE u.uid IN USERS "
#     query2 += "RETURN COLLECT(u.uid) as unwritten"
#     unwritten = __request_database(query2)[0]["unwritten"]

#     return {
#         "uncrawled": uncrawled,
#         "unwritten": unwritten
#     }
