# use server.py to start an instance of the api on wsgi http server for production
# nginx routes to this server through reverse proxy
from server import app

if __name__ == "__main__":
    app.run()
