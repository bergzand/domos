from flask_peewee.admin import Admin
from domos.web.app import app
from domos.web.auth import auth


admin = Admin(app,auth)
admin.register(auth.User)
