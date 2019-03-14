class User(object):
    """
    Basic Twitter user class.  
    :attr id: twitter user id  
    """
    def __init__(self, id: str):
        self.id = id

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
    def __init__(self, id: str, auth_token: str, auth_secret: str, session_token: str):
        super().__init__(id)
        self.auth_token = auth_token
        self.auth_secret = auth_secret
        self.session_token = session_token