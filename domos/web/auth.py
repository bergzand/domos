from flask_peewee.auth import Auth  # Login/logout views, etc.



Auth.initauth = Auth.__init__
def __init__(self):
    Auth.app = None
    Auth.db =  None
Auth.__init__ = __init__
auth = Auth()
