from flask import *
from domos.util.db import *
from domos.util.domossettings import domosSettings

import json
class api:

    def __init__(self,app):
        self.app=app
        self.app.add_url_rule('/api/getmodules','getmodules',self.getmodules,methods=['GET'])
        self.app.add_url_rule('/api/getmodule/<int:module_id>','getmodule',self.getmodule,methods=['GET'])
        
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
    def getmodule(self,module_id):
        db = dbhandler(domosSettings.getDBConfig())
        db.connect()
        module = db.getModuleByID(module_id)
        print(module)
        ret = module.jsjson()
        sensors = []
        print(ret)
        for sensor in module.sensors:
            sens=sensor.jsjson()
            sens.pop('Module')
            sensors.append(sens)
        ret['sensors']=sensors
        print(ret)
        return json.dumps(ret)
            