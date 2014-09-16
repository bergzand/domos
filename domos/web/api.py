from flask import *
from domos.util.db import *
from domos.util.domossettings import domosSettings
import peewee

import json
class api:

    def __init__(self,app):
        self.app=app
        self.app.add_url_rule('/api/getmodules','getmodules',self.getmodules,methods=['GET'])
        self.app.add_url_rule('/api/getmodule/<int:module_id>','getmodule',self.getmodule,methods=['GET'])
        
    def getmodules(self):
        db = dbhandler(domosSettings.getDBConfig())
        db.connect()
        modules = Module.list()
        db.close()
        ret=[]
        for module in modules:
            print(module)
            ret.append(module.to_dict())
        print(ret)
        return json.dumps(ret)
    def getmodule(self,module_id):
        db = dbhandler(domosSettings.getDBConfig())
        db.connect()
        try:
            module = Module.get_by_id(module_id)
        except DoesNotExist:
            return ('404',404)
        ret = module.to_dict()
        sensors = []
        for sensor in module.sensors:
            sens=sensor.to_dict()
            sensors.append(sens)
        ret['sensors']=sensors
        rpcs = []
        for rpc in module.rpcs:
            r=rpc.to_dict()
            r['RPCType']=r['RPCType'].to_dict()
            args =[]
            for arg in rpc.args:
                a = arg.to_dict()
                args.append(a)
            r['args']=args
            rpcs.append(r)
        ret['rpcs']=rpcs
        print(ret)
        return json.dumps(ret)
            