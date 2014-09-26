from domos.util.db import *
from domos.util.domossettings import domosSettings
from flask import Flask, request
from domos.web.views import views
from domos.web.api import API
from domos.util.rpc import *
from domos.util.domoslog import rpchandler


class domosweb:
    def __init__(self, args):
        print(args)
        domosSettings.setConfigFile(args.configfile)
        self.app = None
        self.views = None
        self.api = None
        if (args.webcmd == 'run'):
            self.debug = args.debug
            self.main = self.run
        if (args.webcmd == 'adduser'):
            self.usertoadd = args.user
            self.passwdtoadd = args.passwd
            self.emailtoadd = args.email
            self.admintoadd = args.admin
            self.main = self.adduser

    def addlogger(self):
        try:
            r = rpc("web")
            loghandler = rpchandler(r)
            self.app.logger.addHandler(loghandler)
            self.app.logger.info('connected to logging server')

            @self.app.before_request
            def pre_request_logging():
                # Logging statement
                # if 'text/html' in request.headers['Accept']:
                self.app.logger.debug(' - '.join([
                    request.remote_addr,
                    request.method,
                    request.url,
                    request.headers['Accept'],
                    str(request.data)])
                )
        except exception:
            self.app.logger.error('Could not connect to logging server:' + str(exception))

    def run(self):
        self.app = Flask(__name__)
        self.app.debug = self.debug
        self.app.secret_key = domosSettings.getSecretKey()['secret_key']
        self.app.logger_name = "web"
        self.views = views(self.app)
        self.api = API(self.app)
        self.addlogger()
        self.app.run()

    def adduser(self):
        self.app = Flask(__name__)
        self.app.secret_key = domosSettings.getSecretKey()['secret_key']
        auth.initauth(self.app, self.db)
        newuser = auth.User(username=self.usertoadd, active=True, email=self.emailtoadd, admin=self.admintoadd)
        newuser.set_password(self.passwdtoadd)
        newuser.save()

    @staticmethod
    def parsersettings(parser):
        websubparser = parser.add_subparsers(title='weboption', dest='webcmd')
        adduserparser = websubparser.add_parser('adduser', help='Add a user to the webserver',
                                                description='add an user')
        runparser = websubparser.add_parser('run', help='Add a user to the webserver', description='add an user')
        runparser.add_argument('--debug', '-D', action='store_true', help='Run the webserver in debug mode')

        adduserparser.add_argument('user')
        adduserparser.add_argument('passwd')
        adduserparser.add_argument('--admin', '-A', action='store_true', help='Give the user admin priveleges')
        adduserparser.add_argument('--email', '-m', action='store', help='The email address of the user')
        return parser