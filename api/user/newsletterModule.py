import api.user.mailService as mailService
import api.user.userManager as userManager
from api.neo4j.tracemapUserAdapter import TracemapUserAdapter
userAdapter = TracemapUserAdapter()

def start_save_subscriber( email: str, newsletter_subscribed: bool, beta_subscribed: bool) -> object:
    """
    Starts the subscription status by adding properties for subscriptions
    to the database and sending a confirmation mail to the user if not already
    subscribed for the stuff he/she is subscribing now.  
    :param email: the users email  
    :param newsletter_subscribed: Has the newsletter been subscribed?  
    :param beta_subscribed: Has the beta been subscribed?  
    :returns: object with False for repetitive subscriptions, True for successful subscriptions
    """
    user_status = userAdapter.get_user_status(email)
    response_msg = {}
    # add user if no user exists already
    if not user_status['exists']:
        userAdapter.add_user(email)
    if newsletter_subscribed:
        # check previous newsletter subscriptions
        if user_status['newsletter_subscribed']:
            response_msg['newsletter_subscribed'] = False
            newsletter_subscribed = False
        else:
            response_msg['newsletter_subscribed'] = True
    if beta_subscribed:
        # check previous beta subscriptions
        if user_status['beta_subscribed']:
            response_msg['beta_subscribed'] = False
            beta_subscribed = False
        else:
            response_msg['beta_subscribed'] = True
    if newsletter_subscribed or beta_subscribed:
        # process subscriptions if there are valid ones
        userAdapter.set_user_subscription_status( email, newsletter_subscribed, beta_subscribed)
        confirmation_token = userManager.generate_token()
        userAdapter.set_user_confirmation_token(email, confirmation_token)
        confirmation_link = "https://api.tracemap.info/newsletter/confirm_subscription/%s/%s" % (email, confirmation_token)
        mailService.send_subscription_confirmation_mail(email, confirmation_link, newsletter_subscribed, beta_subscribed)
    return response_msg
    


def save_subscriber( email: str, confirmation_token: str) -> str:
    """
    Check if a user object already exists. If yes
    add the subscription status, if no create user
    and add the subscription status.  
    :param email: the users email  
    :returns: Human readable string to be shown in the browser
    """
    if confirmation_token == userAdapter.get_user_confirmation_token(email):
        if userAdapter.confirm_user_subscription_status(email):
            return "You have successfully confirmed your subscription."
        else:
            return "Something went wrong. Please try to subscribe again."
    else:
        return "This confirmation link is not valid anymore. Please subscribe again at https://tracemap.info"
        


