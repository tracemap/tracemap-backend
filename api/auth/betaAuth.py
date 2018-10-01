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
        return {
            'email': email,
            'password_check': result
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
    if result['password_check']:
        hash = generate_password_hash(new_password)
        db_result = neo4jApi.change_password(email, hash)
        if db_result:
            return {
                'email': email,
                'passwort_changed': True
            }
    else:
        return result