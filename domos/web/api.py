from flask import *
from domos.util.db import *
from domos.util.domossettings import domosSettings

import json
class api:

    def __init__(self,app):
        self.app=app
        self.app.add_url_rule('/api/getmodules','getmodules',self.getmodules,methods=['GET'])
        
    def getmodules(self):
        db = dbhandler(domosSettings.getDBConfig())
        db.connect()
        modules = dbhandler.getModules(db)
        print(modules)
        db.close()
        ret=[]
        for module in modules:
            print(module)
            ret.append(module.jsjson())
        print(ret)
        return json.dumps(ret)
        