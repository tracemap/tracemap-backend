class User(object):
    """
    Basic Twitter user class.  
    :attr user_id: twitter user id  
    """
    def __init__(self, data: dict):
        self.user_id = data['user_id']
        if 'time' in data:
            self.set_time(data['time'])

    def set_time(self, time: str):
        """
        Set this before adding an relation from this user to 
        another user or tweet. It will be used as the relations 
        time property.  
        :param time: unix time string
        """
        self.time = time

class TmUser(User):
    """
    Tracemap user with twitters oauth credentials.  
    :attr id: twitter user id  
    :attr auth_token: twitter oauth token  
    :attr auth_secret: twitter oauth secret
    """
    def __init__(self, data: dict):
        super().__init__(data)
        self.oauth_token = data['oauth_token']
        self.oauth_token_secret = data['oauth_token_secret']
        self.session_token = data['session_token']
        self.screen_name = data['screen_name']