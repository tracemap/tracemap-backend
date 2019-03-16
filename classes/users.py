class User(object):
    """
    Basic Twitter user class.  
    :attr id: twitter user id  
    """
    def __init__(self, data: dict):
        self.id = data['id']
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
        self.auth_token = data['auth_token']
        self.auth_secret = data['auth_secret']
        self.session_token = data['session_token']