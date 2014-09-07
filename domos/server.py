from dashi import DashiConnection
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
import domos.util.domoslog as domoslog
from domos.util.rpc import rpc
from domos.modules.domosTime import domosTime
from domos.handlers import *
import threading
import multiprocessing
import socket
import peewee
import logging
from time import sleep
import pprint
from domos.util.db import *


class messagehandler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.shutdown = False
        self.name = 'domoscore'
        self.rpc = rpc(self.name)
        self.rpc.log_info("starting main thread")
        self.rpc.handle(self.register, "register")
        self.rpc.handle(self.sensorValue, "sensorValue")
        self.rpc.handle(self.addSensor, "addSensor")
        self.rpc.handle(self.getSensorValue, "getSensorValue")
        rpchandle = domoslog.rpchandler(self.rpc)
        self.logger = logging.getLogger('Core')
        self.logger.addHandler(rpchandle)
        self.logger.setLevel(domosSettings.getLoggingLevel('core'))
        self.rpc.log_info("Initializing database")
        db_success = False
        try:
            self.db = dbhandler(domosSettings.getDBConfig())
            self.db.connect()
        except peewee.ImproperlyConfigured as err:
            self.logger.critical("Cannot use database connection: {}".format(err))
        except peewee.OperationalError as err:
            self.logger.critical("Database connection error: {}".format(str(err)))
        if self.db.connected:
            self.db.create_tables()
            self.db.init_tables()
            self.logger.info("Done initializing database")
            self.triggerchecker = triggerChecker(loghandler=rpchandle)
            self.triggerqueue = self.triggerchecker.getqueue()
            self.triggerchecker.start()
            self.actionhandler = actionhandler(self.rpc, loghandler=rpchandle)
            self.actionqueue = self.actionhandler.getqueue()
            self.triggerchecker.setactionqueue(self.actionqueue)
            self.actionhandler.start()
            self.apihandler = apihandler()
            self.apihandler.start()
        else:
            self.shutdown = True
        

    def getSensorValue(self, sensor_id):
        sensor = Sensors.get_by_id(sensor_id)
        return sensor.last()

    def register(self, data=None):
        returnvalue = False
        try:
            module = self.db.getModule(data['name'])
        except DoesNotExist:
            module = self.db.addModule(name=data['name'], queue=data['queue'])
            for rpc in data['rpc']:
                argslist = []
                if "args" in rpc:
                    
                    argslist = [(arg['name'],
                                arg['type'],
                                arg.get('optional', False),
                                arg.get('descr', None)) for arg in rpc['args']]
                self.db.addRPC(module, rpc['key'], rpc['type'], argslist)
        else:
            self.logger.debug("Sending sensors to module")
            return self.db.getModuleSensors(module)

    def addSensor(self, module_id=0, data=None, send=False):
        #add sensor to the database, if send is True, also send it to the module
        self.logger.info('adding sensor from module {0} with ident {1}'.format(module_id, data['ident']))
        module = self.db.getModuleByID(module_id)
        sensor = self.db.addSensor(module, data['ident'], data)
        self.logger.info('Sensor added')
        if send:
            self.sendSensor(module, sensor)

    def sendSensor(self, module, sensor):
        rpccall = self.db.getRPCCall(module, 'add')
        self.rpc.call(module.queue,
                      rpccall.Key,
                      **self.db.getSensorDict(sensor))

    def sensorValue(self, data=None):
        #print(data)
        try:
            self.logger.debug('logging trigger value for {0} with value {1}'.format(data['ident'],data['value']))
            self.db.addValue(data['key'], data['value'])
        except Exception as e:
            self.logger.warn('Something went wrong registering trigger value for {0}: {1}'.format(data['ident'], e))
        else:
            #lauch trigger checks
            self.logger.debug('posting sensordata to trigger processor')
            self.triggerqueue.put(("sensor", data['key'], data['value']))

    def addTrigger(self):
        pass

    def run(self):
        self.logger.info("starting Dashi consumer")
        while not self.shutdown:
            self.rpc.listen()

    def end(self):
        self.shutdown = True

class apihandler(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.shutdown = False
        self.name = 'api'
        self.rpc = rpc(self.name)
        self.rpc.log_info("starting rpc api thread")
        rpchandle = domoslog.rpchandler(self.rpc)
        self.logger = logging.getLogger('api')
        self.logger.addHandler(rpchandle)
        self.logger.setLevel(domosSettings.getLoggingLevel('api'))
        self.rpc.handle(self.listModules, "getModules")
        self.db = dbhandler()
        
    def listModules(self):
        modules = [[module.name,
                    module.queue,
                    module.Active] for module in self.db.getModules()]
        return modules
    
    def listSensors(self, module=None):
        pass
    
    def run(self):
        self.logger.info("start consuming api calls")
        while not self.shutdown:
            self.rpc.listen()


class domos:
    def __init__(self, args):
        self.args = args
        self.configfile = args.configfile
        ds.domosSettings.setConfigFile(self.configfile)

    def main(self):
        logger = domoslog.rpclogger()
        logger.start()
        msgh = messagehandler()
        if msgh.shutdown:
            logger.log_critical("Initialization error, shutting down", "domoscore")
            logger.end()
        else:
            msgh.start()
            dt = domosTime()
            dt.start()
            msgh.rpc.log_info('waiting for modules to register')
            msgh.join()
