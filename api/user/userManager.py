from werkzeug.security import generate_password_hash
from werkzeug.security import check_password_hash
import string
import random
import os
import api.user.mailService as mailService
from api.neo4j.tracemapUserAdapter import TracemapUserAdapter
userAdapter = TracemapUserAdapter()

def __get_beta_users() -> set:
    """
    Builds a set from the email adresses in beta_users.txt  
    :returns: the set containing all beta users
    """
    beta_users_set = set()
    with open('user-data/beta_users.txt') as f:
        beta_users_list = f.readlines()
        for email in beta_users_list:
            beta_users_set.add(email.lower())
    return beta_users_set


def __generate_random_pass() -> str:
    """
    Generates a random password string with length 10  
    :returns: the password string
    """
    chars = string.ascii_lowercase + string.digits
    size = 10
    return ''.join(random.choice(chars) for x in range(size))

def generate_token() -> str:
    """
    Generates a random token string with length 30  
    :returns: the token string
    """
    chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
    size = 30
    return ''.join(random.choice(chars) for x in range(size))

def register_user(username: str, email: str) -> object:
    """
    Register a user for the app by setting its username and password hash if
    the email adress is not already registered.  
    :param username: the users username  
    :param email: the users email  
    :returns: an object containing {'is_created': True} or {'error': <the error message displayed in the frontend>}
    """
    if email in __get_beta_users():
        password = __generate_random_pass()
        password_hash = generate_password_hash(password)
        user_status = userAdapter.get_user_status(email)
        if not user_status['exists']:
            userAdapter.add_user(email)
        if not user_status['registered']:
            mail_response = mailService.send_credentials_mail(username, email, password)
            if 'error' in mail_response:
                return {
                    'error': 'mail: ' + mail_response['error']
                }
            else:
                db_response = userAdapter.set_user_username_password(username, email, password_hash)
                return {
                    'is_created': db_response,
                }
        else:
            return {
                'error': 'This email is already taken.'
            }
    else:
        return {
            'error': 'This email adress is not valid for the closed beta.'
        }
    
    

def check_session(email: string, session_token: string) -> bool:
    """
    Gets the valid saved session_token from the database
    and if it exists, compares it with the given session_token  
    :param email: the users email
    :param session_token: the input session_token
    :returns: The boolean result of the check
    """
    db_session_token = userAdapter.get_user_session_token(email)
    return db_session_token == session_token


def check_password(email: str, password:str) -> object:
    """
    checks a user password against the saved password_hash
    for that user in the database.  
    :param email: the users email  
    :param password: the users password  
    :returns: on success {'session_token': <the generated or updated session_token>}
    else {'error': 'Wrong password.'} or {'error': 'Wrong email.'}
    """
    db_password_hash = userAdapter.get_user_password_hash(email)
    if db_password_hash:
        if check_password_hash(db_password_hash, password):
            db_session_token = userAdapter.get_user_session_token(email)
            if db_session_token:
                session_token = db_session_token
            else:
                session_token = generate_token()
                userAdapter.set_user_session_token(email, session_token)
            return {
                'session_token': session_token
            }
        else:
            return {
                'error': 'Wrong password.'
            }
    else:
        return {
            'error': 'This email is not registered.'
        }

def delete_user(email: str, password: str) -> object:
    """
    Deletes the user from the database if the provided
    password is correct  
    :param email: the users email  
    :param password: the users password  
    :returns: {'deleted': True} on success else {'error': 'Wrong password.'}
    """
    result = check_password(email, password)
    if result['session_token']:
        if userAdapter.delete_user(email):
            return {
                'deleted': True
            }
        else:
            return {
                'error': 'unknown error.'
            }  
    else:
        return result

def change_password(email:str, old_password: str, new_password: str) -> object:
    """
    Changes the password of a user if the old_password is correct  
    :param email: the users email  
    :param old_password: the users actual password  
    :param new_password: the users new password  
    :returns: on success {'password_changed': True} else
    {'error': 'Your old password is wrong.'}
    """
    # no need to check if email is correct, because user is already logged in
    if check_password(email, old_password):
        hash = generate_password_hash(new_password)
        response = userAdapter.set_user_password_hash(email, hash)
        if response:
            return {
                'passwort_changed': response
            }
        else:
            return {
                'error': 'unknown error'
            }
    else:
        return {
            'error': 'Your old password is wrong.'
        }

def request_reset_user(email: str) -> object:
    """
    Request a user reset with sending out a mail with a reset link to this users email  
    :param email: the users email  
    :returns: True on success, False on Error
    """
    reset_token = generate_token()
    if userAdapter.set_user_reset_token(email, reset_token):
        username = userAdapter.get_user_username(email)
        link = 'https://api.tracemap.info/auth/reset_password/%s/%s' % (email, reset_token)
        return mailService.send_reset_mail(username, email, link)
    else:
        return False


def reset_password(email: str, reset_token: str) -> str:
    """
    Reset a users password after checking if the reset_token is correct
    and send out a new password to that user  
    :param email: the users email  
    :param reset_token: the reset_token for checking against the database  
    :returns: Human readable string to be displayed in the users browser
    """
    db_reset_token = userAdapter.get_user_reset_token(email)
    if reset_token == db_reset_token:
        password = __generate_random_pass()
        password_hash = generate_password_hash(password)
        userAdapter.set_user_password_hash(email, password_hash)
        username = userAdapter.get_user_username(email)
        mailService.send_new_password(username, email, password)
        return 'You received an email with a new password.'
    else:
        return 'The request token did not match. Please request a new passwort reset at https://tracemap.info'



def get_username(email:string) -> str:
    """
    check the session and return the users username on success  
    :param email: the users email 
    :param session_token: the users session_token  
    :returns: the users username string
    """
    return userAdapter.get_user_username(email)