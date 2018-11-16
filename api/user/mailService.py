import smtplib
import os
from email.mime.text import MIMEText

def send_credentials_mail(username: str, email: str, password: str) -> object:
    """
    Send the register verfification mail to a users email  
    :param username: the users username  
    :param email: the users email  
    :param password: the users initial password  
    :returns: object containing {'mail_sent': True} or {'error':<error-string>}  
    """
    msg_string = """
    Hey {username},
    thank you for participating in our closed beta.

    Your user account has been successfully created.

    Your credentials for logging in are:
    #########################################
    ### email: {email}
    ### password: {password}
    #########################################

    We recommend you to change your password in the user menu which appears
    next to the menu button as soon as you are logged in.

    After using the tool and finding around, we would like to
    get your feedback about it here:
    https://goo.gl/forms/37vROtjqgL6nexyt2
    (it will take you ~ 15min.)
    Thanks for the feedback!

    If you got questions regarding the beta you can answer this mail.
    For any non-beta questions, please use contact@tracemap.info

    Best,
    the TraceMap team.
    """.format(
        email=email,
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

def send_reset_mail(username: str, email: str, reset_link: str) -> bool:
    """
    Send the password reset link to the user after a reset has been requested.  
    :param username: the users username  
    :param email: the users email  
    :param link: the reset_link to finish the password reset  
    :returns: True on success, False on Error
    """
    msg_string = """
    Hey {username},
    a reset of your password has been requested.

    Click the following link to receive an e-mail with your new password:
    {link}
    (The link is valid for one day.)

    If you didn't request a new password for your account, you can ignore this mail.

    Best,
    the TraceMap team.
    """.format(
        username=username,
        link=reset_link
    )
    msg = MIMEText(msg_string)
    msg['Subject'] = 'How to reset your password.'
    msg['From'] = 'beta@tracemap.info'
    msg['To'] = email
    try: 
        email_user = os.environ.get('BETA_MAIL_ADDRESS')
        email_password = os.environ.get('BETA_MAIL_PASSWORD')
        smtpObj = smtplib.SMTP( 'smtp.strato.de', 587)
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.ehlo()
        smtpObj.login(email_user, email_password)
        smtpObj.sendmail('beta@tracemap.info', email, msg.as_string())
        smtpObj.quit()
        return True
    except Exception as error:
        return {
            'error': str(error)
        }

def send_subscription_confirmation_mail(email: str, confirmation_link: str, newsletter_checked: bool, beta_checked: bool) -> object:
    """
    Send the subscription confirmation link to the user 
    along with a message explaining beta related stuff.  
    :param email: the users email  
    :param confirmation_link: the confirmation_link  
    :param newsletter_checked: True if the user subscribed to the newsletter  
    :param beta_checked: True if the user subscribed to the beta
    """
    if newsletter_checked and beta_checked:
        msg_string = """
        Hey Subscriber,
        Thank you for subscribing to our newsletter and applying for our closed beta.

        To confirm that you own this email address and complete the subscription, please click the following link:
        {link}
        (The link is valid for one day.)

        Please keep in mind that we invite users based on our resources.
        Our tool is in an early stage and we are testing the performance of our system with an increasing number of users. 
        Therefore it can take some time until your email address is added to the pool of beta users.

        As soon as we activate your email address you will receive a confirmation email.

        If you did not subscribe on our website, you can ignore this mail.

        Best,
        the TraceMap team.
        """.format(
            link=confirmation_link
        )
    elif newsletter_checked:
        msg_string = """
        Hey Subscriber,
        Thank you for subscribing to our newsletter.

        To confirm that you own this email address and complete the subscription, please click the following link:
        {link}
        (The link is valid for one day.)

        If you did not subscribe on our website, you can ignore this mail.

        Best,
        the TraceMap team.
        """.format(
            link=confirmation_link
        )
    elif beta_checked:
        msg_string = """
        Hey Subscriber,
        Thank you for applying for our closed beta.

        To confirm that you own this email address and complete the subscription, please click the following link:
        {link}
        (The link is valid for one day.)

        Please keep in mind that we invite users based on our resources.
        Our tool is in an early stage and we are testing the performance of our system with an increasing number of users. 
        Therefore it can take some time until your email address is added to the pool of beta users.

        As soon as we activate your email address you will receive a confirmation email.

        If you did not subscribe on our website, you can ignore this mail.

        Best,
        the TraceMap team.
        """.format(
            link=confirmation_link
        )
        
    msg = MIMEText(msg_string)
    msg['Subject'] = 'Thanks for subscribing, please finish your subscription now.'
    msg['From'] = 'beta@tracemap.info'
    msg['To'] = email
    try: 
        email_user = os.environ.get('BETA_MAIL_ADDRESS')
        email_password = os.environ.get('BETA_MAIL_PASSWORD')
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

def send_new_password(username: str, email: str, new_password: str) -> object:
    """
    Send a new password to the user after a successfull reset  
    :param username: the users username  
    :param email: the users email  
    :param new_password: the users new password  
    :returns: object containing {'mail_sent': True} or {'error':<error-string>}
    """
    msg_string = """
    Hey {username},
    we reset your password

    Your new credentials for logging in are:
    #########################################
    ### email: {email}
    ### password: {password}
    #########################################

    We recommend you to change your password in the user menu which appears
    next to the menu button as soon as you are logged in.

    Best,
    the TraceMap team.
    """.format(
        email=email,
        username=username,
        password=new_password
    )
    msg = MIMEText(msg_string)
    msg['Subject'] = 'Hey %s. Here is your new TraceMap password.' % username
    msg['From'] = 'beta@tracemap.info'
    msg['To'] = email
    try: 
        email_user = os.environ.get('BETA_MAIL_ADDRESS')
        email_password = os.environ.get('BETA_MAIL_PASSWORD')
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