from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.exceptions import CypherError
import json
import time
import os

class TracemapUserAdapter:

    def __init__(self):
        uri = os.environ.get('NEO4J_URI')
        self.driver = GraphDatabase.driver(uri, auth=(os.environ.get('NEO4J_USER'),
        os.environ.get('NEO4J_PASSWORD')))
    
    def __request_database(self, query: str) -> object:
        """
        Takes a query and returns the neo4j data response of that query  
        :param query: the query string  
        :returns: database response data object
        """
        with self.driver.session() as session:
            with session.begin_transaction() as transaction:
                try:
                    response_data = transaction.run(query).data()
                    return response_data
                except CypherError as e:
                    return {'error': self.__check_database_error_code(e.code)}

    @staticmethod
    def __check_database_error_code(code):
        return {
            'Neo.ClientError.Schema.ConstraintValidationFailed': 'Constraint Error'
        }.get(code, "unhandled error %s" % code)

    def check_session(self, user_name: str, session_token: str) -> bool:
        """
        Gets the valid saved session_token from the database
        and if it exists, compares it with the given session_token  
        :param email: the users email
        :param session_token: the input session_token
        :returns: The boolean result of the check
        """
        db_session_token = self.get_user_session_token(user_name)
        return db_session_token == session_token

    def get_user_status(self, email: str) -> object:
        """
        Check the status of a user by its email.  
        :param email: the users email  
        :returns: a dict containing boolean values for
        'exists', 'beta_subscribed', 'newsletter_subscribed', 'registered' 
        """
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "RETURN {username: u.username, newsletter_subscribed: u.newsletter_subscribed, beta_subscribed: u.beta_subscribed} as user_status"
        response = self.__request_database(query)
        if response:
            user_data = response[0]['user_status']
            return {
                'exists': True,
                'registered': bool(user_data['username']),
                'newsletter_subscribed': user_data['newsletter_subscribed'],
                'beta_subscribed': user_data['beta_subscribed']
            }
        else:
            return {
                'exists': False,
                'registered': False,
                'newsletter_subscribed': False,
                'beta_subscribed': False
            }

    def add_user(self, email: str) -> bool:
        """
        Create the basic user object in the database.
        This user is just identified by its email and
        has no access to the tool if the password and username
        properties are not set.
        :param email: the users email
        :returns: True on success, False if user for this email already exists
        """
        query = "CREATE (u:TracemapUser {email: '%s'})" % email
        response = self.__request_database(query)
        if 'error' in response:
            return False
        else:
            return True

    def set_user_username_password(self, username: str, email: str, password_hash: str) -> bool:
        """
        Adds a user to the database  
        :param username: the users name  
        :param email: the users email  
        :param password_hash: a hash of that users password  
        :returns: True on success, False if user does not exist
        """
        query = "MATCH (u:TracemapUser {email: '%s'}) " % email
        query += "SET u.username = '%s', u.password_hash = '%s' " % (username, password_hash)
        query += "RETURN u"
        response = self.__request_database(query)
        return bool(response)

    def get_user_username(self, email: str) -> str:
        """
        Returns the email and username of a user identified by the email  
        :param email: the users email  
        :returns: the username string or an empty string if user does not exist
        """
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' RETURN u.username" % email
        database_response = self.__request_database(query)
        if database_response:
            return database_response[0]['u.username']
        else:
            return ""

    def get_user_password_hash(self, email: str) -> str:
        """
        Get the password_hash of a user identified by the email.
        :param email: the users email  
        :returns: the users password_hash or an empty string if user does not exist
        """
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' RETURN u.password_hash" % email
        database_response = self.__request_database(query)
        if database_response and 'u.password_hash' in database_response[0]:
            return database_response[0]['u.password_hash']
        else:
            return ""

    def set_user_session_token(self, email: str, session_token: str) -> bool:
        """
        Saves the users session_token and adds a unix timestamp
        for determining the age of the token at any later time  
        :param email: the users email  
        :param session_token: the users session_token  
        :returns: True on success
        """
        timestamp = time.time()
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "SET u.session_token = '%s' " % session_token
        query += "SET u.session_timestamp = %s" % timestamp
        self.__request_database(query)
        return True

    def get_user_session_token(self, email: str) -> str:
        """
        Get the session_token of a user if there is a valid one.  
        :param email: the users email  
        :returns: session_token string if there is a valid one, empty string if not
        """
        two_hours = 60 * 120
        timestamp = time.time()
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "RETURN u.session_timestamp, u.session_token"
        database_response = self.__request_database(query)
        if database_response:
            old_timestamp = database_response[0]['u.session_timestamp']
            if (not old_timestamp) or (old_timestamp < timestamp - two_hours):
                # delete token and return error: expired
                query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
                query += "REMOVE u.session_token, u.session_timestamp"
                self.__request_database(query)
                return ""
            else:
                # renew timestamp and return session_token
                query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
                query += "SET u.session_timestamp = %s " % timestamp
                self.__request_database(query)
                return database_response[0]['u.session_token']
        else:
            # return error: no token
            return ""

    def set_user_reset_token(self, email: str, reset_token: str) -> bool:
        """
        Saves the users reset_token and adds a unix timestamp
        for determining the age of the token at any later time  
        :param email: the users email  
        :param reset_token: the users reset_token  
        :returns: True on success, False if user does not exist
        """
        timestamp = time.time()
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "SET u.reset_token = '%s' " % reset_token
        query += "SET u.reset_timestamp = %s " % timestamp
        query += "RETURN u"
        response = self.__request_database(query)
        return bool(response)


    def get_user_reset_token(self, email: str) -> object:
        """
        Get the reset_token of a user if there is a valid one.  
        :param email: the users email  
        :returns: reset_token if there is one, human readable error for display in the browser if not.
        """
        one_day = 60 * 60 * 24
        timestamp = time.time()
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "RETURN u.reset_timestamp, u.reset_token"
        database_response = self.__request_database(query)
        if database_response:
            reset_token = database_response[0]['u.reset_token']
            old_timestamp = database_response[0]['u.reset_timestamp']
            if not reset_token:
                return {
                    'error': 'The reset token does not exist. Please request a password reset at https://tracemap.info.'
                }
            else:
                if old_timestamp < timestamp - one_day:
                    # delete token and return expired error message
                    return {
                        'error': 'The link is expired. Please request a new password reset at https://tracemap.info.'
                    }
                else:
                    return {
                        'token': reset_token
                    }
                query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
                query += "REMOVE u.reset_token, u.reset_timestamp"
                self.__request_database(query)
        else:
            # return undefined error
            return {
                'error': 'User does not exist.'
            }


    def delete_user(self, email: str) -> bool:
        """
        Delete TracemapUser nodes registration properties (especially password_hash and username).
        Dont delete the whole node because of subscriptions.
        :param email: the users email  
        :returns: True on success
        """
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "REMOVE u.username, u.password_hash, u.session_token, u.session_timestamp"
        self.__request_database(query)
        return True

    def set_user_password_hash(self, email: str, new_password_hash: str) -> bool:
        """
        Change a users password_hash to a new value.  
        :param email: the users email  
        :param new_password_hash: the new passwords hash  
        :returns: True on success, False if user does not exist
        """
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "SET u.password_hash = '%s' " % new_password_hash
        query += "RETURN u"
        response = self.__request_database(query)
        return bool(response)

    def set_user_subscription_status(self, email: str, newsletter_subscribed: bool, beta_subscribed: bool) -> bool:
        """
        Sets the user subscription status to 0 for unconfirmed subscriptions.  
        :param email: the users email  
        :param newsletter_subscribed: has the user subscribed to the newsletter?  
        :param beta_subscribed: has the user subscribed to the beta?  
        :returns: True on success, False on Error
        """
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        if newsletter_subscribed:
            query += "SET u.newsletter_subscribed = 0 "
        if beta_subscribed:
            query += "SET u.beta_subscribed = 0 "
        query += "RETURN u"
        response = self.__request_database(query)
        return bool(response)
    
    def confirm_user_subscription_status(self, email: str) -> bool:
        """
        Confirms the subscription but changing the value of the subscription properties
        from 0 to True.  
        :param email: the users email  
        :returns: True for success, False if user does not exist
        """
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "RETURN {newsletter_subscribed: u.newsletter_subscribed, beta_subscribed: u.beta_subscribed} as subscription_status"
        response = self.__request_database(query)
        if response:
            subscription_status = response[0]['subscription_status']
            query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
            if subscription_status['newsletter_subscribed'] == 0:
                query += "SET u.newsletter_subscribed = True "
            if subscription_status['beta_subscribed'] == 0:
                query += "SET u.beta_subscribed = True "
            query += "REMOVE u.confirmation_token, u.confirmation_timestamp"
            self.__request_database(query)
            return True
        else:
            return False

    def set_user_confirmation_token(self, email: str, confirmation_token: str) -> bool:
        """
        Set a users confirmation token used for identifying the users subscription confirmation link.  
        :param email: the users email  
        :param confirmation_token: the generated confirmation_token  
        :returns: True on success, False if user does not exist
        """
        timestamp = time.time()
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "SET u.confirmation_token = '%s' " % confirmation_token
        query += "SET u.confirmation_timestamp = %s " % timestamp
        query += "RETURN u"
        response = self.__request_database(query)
        return bool(response)

    def get_user_confirmation_token(self, email: str) -> str:
        """
        Get the confirmation_token of a user if there is a valid one.  
        :param email: the users email  
        :returns: confirmation_token if there is one, human readable error for display in the browser if not.
        """
        one_day = 60 * 60 * 24
        now_timestamp = time.time()
        query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
        query += "RETURN u.confirmation_token, u.confirmation_timestamp"
        database_response = self.__request_database(query)
        if database_response:
            confirmation_token = database_response[0]['u.confirmation_token']
            confirmation_timestamp = database_response[0]['u.confirmation_timestamp']
            if not confirmation_timestamp:
                # return error: no token found
                return {
                    'error': 'Something went wrong. Please try to subscribe again at https://tracemap.info'
                }
            else:
                if confirmation_timestamp < now_timestamp - one_day:
                    # delete token and return expired error
                    return {
                        'error': 'This confirmation link is not valid anymore. Please subscribe again at https://tracemap.info'
                    }
                else:
                    return {
                        'token': confirmation_token
                    }
                query = "MATCH (u:TracemapUser) WHERE u.email = '%s' " % email
                query += "REMOVE u.confirmation_token, u.confirmation_timestamp"
                self.__request_database(query)
        else:
            # return undefined error
            return {
                'error': 'User does not exist.'
            }