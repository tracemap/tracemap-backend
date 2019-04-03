from neo4j import GraphDatabase, basic_auth
from classes.users import User, TmUser
from classes.tweets import Tweet, TraceMap
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

    def write_oauth_user(self, data: dict) -> dict:
        """
        Save the oauth credentials of a login request  
        :param data: the dict holding oauth_token and oauth_token_secret
        """
        cypher_statement = """
        CREATE (user:Auth)
        SET user = {props}
        """
        cypher_params = {'props': data}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def get_oauth_user_secret(self, oauth_token: str) -> object:
        """
        Search the :Auth user with the given oauth_token 
        and return this users oauth_token_secret.  
        :param oauth_token: The users oauth_token
        """
        cypher_statement = """
        MATCH (user:Auth{oauth_token:{oauth_token}}) 
        RETURN {oauth_token_secret: user.oauth_token_secret} as data
        """
        cypher_params = {'oauth_token': oauth_token}
        result = self.__request_database(cypher_statement, cypher_params)
        if result == []:
            return None
        else:
            return result[0]['data']

    def delete_oauth_user(self, oauth_token: str) -> object:
        """
        Search the :Auth user node and delete it.  
        :param oauth_token: the users request token
        """
        cypher_statement = """
        MATCH (u:Auth{oauth_token:{oauth_token}})  
        DETACH DELETE u
        """
        cypher_params = {'oauth_token': oauth_token}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def write_tm_user(self, tmUser: TmUser) -> dict:
        """
        Create a new tracemap user or alter existing user to 
        tracemap user.  
        :param user_id: the user_id from twitter  
        :param twitter_token: the users oauth token from twitter  
        :param twitter_secret: the users oauth secret from twitter  
        :param session_token: the users session_token from browser storage
        """
        cypher_statement = """
        MERGE (user:User{user_id:{user_id}}) 
        SET user = {props} 
        SET user:TmUser
        """
        cypher_params = {'user_id': tmUser.user_id, 'props': tmUser.__dict__}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def write_retweeters(self, tweet_id: str, users: List[User]) -> dict:
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

    def write_author( self, tweet_id: str, author: User) -> dict:
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

    def write_tweet_request(self, tweet_id: str, requester: TmUser) -> dict:
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

    def write_user_followships(self, source_id: dict, targets: list) -> dict:
        """
        Add a list of :FOLLOWS relations between users in the database.  
        :param source_id: the source user (user following the list of users) id.  
        :param targets: a list of target ids for the :FOLLOWS relation.
        """
        time_now = str(int(time.time()))
        cypher_statement = """
        MATCH (source:User{id:{source_id}}) 
        WITH {targets} AS targets, source 
        UNWIND targets as target_id 
        MERGE (source)-[f:FOLLOWS]->(:User{id:target_id}) 
        SET f.time = {time}
        """
        cypher_params = {
            'source_id': source_id, 
            'targets': targets, 
            'time': time_now}
        self.__request_database(cypher_statement, cypher_params)
        return {'success': True}

    def get_tm_user(self, user_id: str) -> TmUser:
        """
        Get the registered TmUser by id.  
        :param id: the users id
        """
        cypher_statement = """
        MATCH (user:TmUser{user_id:{user_id}}) 
        RETURN properties(user) as data
        """
        cypher_params = {'user_id': user_id}
        result = self.__request_database(cypher_statement, cypher_params)
        if result == []:
            return None
        else:
            user_dict = result[0]['data']
            return TmUser( user_dict)

    def get_tweet_data(self, tweet_id: str, time_limit: str='') -> Tweet:
        """
        Get the tweet object with all connected nodes (except requested). 
        The optional time_limit limits the returned retweeters to those 
        retweeted before time_limit.  
        :param tweet_id: the tweets id
        :param time_limit (optional): the time_limit timestring
        """
        cypher_statement = """
        MATCH (tweet:Tweet{id:{tweet_id}})<-[retweeted:RETWEETED]-(retweeter:User), 
        (tweet:Tweet{id:'847112'})<-[authored:AUTHORED]-(author:User) 
        WITH tweet, {time: retweeted.time, id: retweeter.id} as retweets, 
        {time: authored.time, id: author.id} as author 
        RETURN {id: tweet.id, last_updated: tweet.last_updated, retweeters: collect(retweets), author: author} as data 
        """
        cypher_params = {'tweet_id': tweet_id}
        result = self.__request_database(cypher_statement, cypher_params)
        if result == []:
            return None
        else:
            data = result[0]['data']
            data['author'] = User(data['author'])
            if time_limit:
                time_limit = float(time_limit)
                data['retweeters'] = list(filter(lambda x: float(x['time']) < time_limit, data['retweeters']))
            data['retweeters'] = list(map(lambda x: User(x), data['retweeters']))
            return Tweet(data)

    def get_user_followships(self, tweet_id: str) -> list:
        """
        Get the followship paths between the retweeters & author of a tweet.  
        :param tweet_id: str
        """
        cypher_statement = """
        MATCH (:Tweet{id:{id}})<-[:AUTHORED|:RETWEETED]-(user:User) 
        WITH COLLECT(user.id) as user_ids
        MATCH (target:User)<-[:FOLLOWS]-(source:User) 
        WHERE target.id IN user_ids AND source.id IN user_ids
        RETURN COLLECT({target: target.id, source: source.id}) as data
        """
        cypher_params = {'id': tweet_id}
        result = self.__request_database(cypher_statement, cypher_params)
        if result == []:
            return None
        else:
            return result[0]['data']
