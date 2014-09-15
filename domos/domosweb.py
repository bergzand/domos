from domos.util.db   import *
from domos.util.domossettings import domosSettings
from flask import Flask
from domos.web.views import views
from domos.web.api import api

class domosweb:
    
    def __init__(self,args):
        print(args)
        domosSettings.setConfigFile(args.configfile)
        if(args.webcmd == 'run'):
            self.debug = args.debug
            self.main = self.run
        if(args.webcmd == 'adduser'):
            self.usertoadd = args.user
            self.passwdtoadd = args.passwd
            self.emailtoadd = args.email
            self.admintoadd = args.admin
            self.main = self.adduser

    def run(self):
         self.app = Flask(__name__)
         self.app.debug = self.debug;
         self.app.secret_key = domosSettings.getSecretKey()['secret_key']
         self.views = views(self.app)
         self.api = api(self.app)
         self.app.run()
         
    def adduser(self):
         self.app = Flask(__name__)
         self.app.secret_key = domosSettings.getSecretKey()['secret_key']
         auth.initauth(self.app,self.db)
         newuser = auth.User(username = self.usertoadd,active=True, email = self.emailtoadd, admin=self.admintoadd)
         newuser.set_password(self.passwdtoadd)
         newuser.save()
