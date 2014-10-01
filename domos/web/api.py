from flask import *
from domos.util.db import *
from domos.util.domossettings import domosSettings
import peewee

import json


def getmodules():
    db = dbhandler(domosSettings.getDBConfig())
    db.connect()
    modules = Module.list()
    db.close()
    ret = []
    for module in modules:
        print(module)
        ret.append(module.to_dict())
    print(ret)
    return json.dumps(ret)


class API:
    def __init__(self, app):
        self.app = app
        self.app.add_url_rule('/api/getmodules', 'getmodules', getmodules, methods=['GET'])
        self.app.add_url_rule('/api/getmodule/<int:module_id>', 'getmodule', self.getmodule, methods=['GET'])

    def getmodule(self, module_id):
        db = dbhandler(domosSettings.getDBConfig())
        db.connect()
        try:
            module = Module.get_by_id(module_id)
        except DoesNotExist:
            return ('404', 404)
        ret = module.to_dict(deep=['sensors', 'rpcs', 'args','rpctype'])
        print(ret)
        return json.dumps(ret)
