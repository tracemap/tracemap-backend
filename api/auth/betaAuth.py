from werkzeug.security import generate_password_hash
from werkzeug.security import check_password_hash
import api.neo4j.neo4jApi as neo4jApi
import string
import random
import os
import smtplib
from email.mime.text import MIMEText

beta_users = []

with open('user-data/beta_users.txt') as f:
    beta_users = f.readlines()
    beta_users = [x.strip() for x in beta_users]

def __generate_random_pass():
    chars = string.ascii_lowercase + string.digits
    size = 8
    return ''.join(random.choice(chars) for x in range(size))

def __generate_session_token():
    chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
    size = 30
    return ''.join(random.choice(chars) for x in range(size))

def __add_new_user(username: string, email: string):
    if email in beta_users:
        password = __generate_random_pass()
        hash = generate_password_hash(password)
        check_user = neo4jApi.get_beta_user_data(email)
        if 'error' in check_user:
            user_obj = {
                'username': username,
                'email': email,
                'hash': hash 
            }
            mail_response = __send_verification_mail(username, email, password)
            if 'error' in mail_response:
                return {
                    'email': email,
                    'error': 'mail: ' + mail_response['error']
                }
            else:
                db_response = neo4jApi.add_beta_user(user_obj)
                return {
                    'email': email,
                    'is_created': db_response,
                }
        else:
            return {
                'email': email,
                'error': 'user already exists'
            }
    else:
        return {
            'email': email,
            'error': 'email is not a beta tester'
        }
    
def __send_verification_mail(username, email, password):
    msg_string = """
    Hey {username},
    thank you for participating in our closed beta.

    Your User Account has been successfully created.

    Your Credentials for logging in are:
    #########################################
    ### username: {username}
    ### password: {password}
    #########################################

    We recommend you to change your password in the user menu which appears
    next to the menu button as soon as you are logged in.

    After using the Tool and finding around, we would like to
    get your feedback about it here:
    https://link-to-survey.de
    (it will take you ~ 15min.)
    Thanks for the feedback!

    If you got questions regarding the beta you can answer this mail.
    For any non-beta questions, please use contact@tracemap.info

    Best,
    Allegra, Bruno, Jonathan & Eike from TraceMap.
    """.format(
        username=username,
        password=password
    )
    msg = MIMEText(msg_string)
    msg['Subject'] = 'Welcome %s! Your TraceMap beta Account was created.' % username
    msg['From'] = 'beta@tracemap.info'
    msg['To'] = email
    try: 
        email_user = os.environ.get('BETA_MAIL_ADDRESS')
        email_password = os.environ.get('BETA_MAIL_PASSWORD')
        print( email_user + email_password)
        smtpObj = smtplib.SMTP( 'smtp.strato.de', 587)
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.ehlo()
        smtpObj.login(email_user, email_password)
        smtpObj.sendmail('beta@tracemap.info', email, msg.as_string())
        smtpObj.quit()
        return {
            "mail_sent": True
        }
    except Exception as error:
        return {
            'error': str(error)
        }

def sendResetMail(email: string, link: string):
    userdata = neo4jApi.get_beta_user_data(email)
    if 'error' in userdata:
        return {
            'error': 'email does not exist'
        }
    else:
        username = userdata['u.username']
        msg_string = """
        Hey {username},
        a reset of your password has been requested.

        Click the following link to receive an email with your new password:
        {link}

        If you didn't request a new password for your account, you can ignore this mail.

        Best,
        Allegra, Bruno, Jonathan & Eike from TraceMap.
        """.format(
            username=username,
            link=link
        )
        msg = MIMEText(msg_string)
        msg['Subject'] = 'How to reset your password.'
        msg['From'] = 'beta@tracemap.info'
        msg['To'] = email
        try: 
            email_user = os.environ.get('BETA_MAIL_ADDRESS')
            email_password = os.environ.get('BETA_MAIL_PASSWORD')
            print( email_user + email_password)
            smtpObj = smtplib.SMTP( 'smtp.strato.de', 587)
            smtpObj.ehlo()
            smtpObj.starttls()
            smtpObj.ehlo()
            smtpObj.login(email_user, email_password)
            smtpObj.sendmail('beta@tracemap.info', email, msg.as_string())
            smtpObj.quit()
            return {
                "reset_mail_sent": True
            }
        except Exception as error:
            return {
                'error': str(error)
            }
    

def __simple_check_session(email: string, session_token: string):
    db_session = neo4jApi.get_user_session_token(email)
    if 'error' in db_session:
        return False
    elif db_session['token'] == session_token:
        return True
    else:
        return False


def check_password(email: string, password:string):
    db_response = neo4jApi.get_beta_user_hash(email)
    if 'error' in db_response:
        return {
            'email': email,
            'error': 'user does not exist',
            'password_check': False
        }
    else:
        hash = db_response
        result = check_password_hash(hash, password)
        if result:
            session_token = __generate_session_token()
            db_session = neo4jApi.get_user_session_token(email)
            if 'token' in db_session:
                session_token = db_session['token']
            else:
                neo4jApi.set_user_session_token(email, session_token)
            return {
                'email': email,
                'password_check': result,
                'session_token': session_token
            }
        else:
            return {
                'email': email,
                'error': 'wrong password'
            }

def check_session(email: string, session_token: string):
    db_session = neo4jApi.get_user_session_token(email)
    if 'error' in db_session:
        return db_session
    elif db_session['token'] == session_token:
        return {
            'session': True
        }
    else:
        return {
            'error': 'tokens do not match'
        }


def add_user(username: string, email: string):
    return __add_new_user(username, email)

def delete_user(email: string, password: string):
    result = check_password(email, password)
    if result['password_check']:
        db_result = neo4jApi.delete_beta_user(email)
        if db_result:
            return {
                'email': email,
                'deleted': True
            }
    else:
        return result

def change_password(email:string, password: string, new_password: string):
    result = check_password(email, password)
    if 'password_check' in result:
        hash = generate_password_hash(new_password)
        db_result = neo4jApi.change_password(email, hash)
        if db_result:
            return {
                'email': email,
                'passwort_changed': True
            }
    else:
        return result

def request_reset_user(email: string):
    reset_token = __generate_session_token()
    neo4jApi.set_user_reset_token(email, reset_token)
    link = 'https://api.tracemap.info/auth/reset_password/%s/%s' % (email, reset_token)
    return(sendResetMail(email, link))


def reset_password(email: string, reset_token: string):
    db_response = neo4jApi.get_user_reset_token(email)
    if 'token' in db_response:
        if reset_token == db_response['token']:
            password = __generate_random_pass()
            hash = generate_password_hash(password)
            neo4jApi.change_password(email, hash)
            user_data = neo4jApi.get_beta_user_data(email)
            username = user_data['u.username']
            __send_verification_mail(username, email, password)
            return 'You received an e-mail with a new password'
        else:
            return 'The request token did not match. Please request a new passwort reset at https://tracemap.info'
    elif 'error' in db_response:
        return db_response['error']



def get_user_data(email:string, session_token: string):
    if __simple_check_session(email, session_token):
        return neo4jApi.get_beta_user_data(email)