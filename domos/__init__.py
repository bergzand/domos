from dashi import DashiConnection
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
from domos.util.domoslog import rpclogger
from domos.util.rpc import rpc
from domos.modules.domosTime import domosTime
from domos.util.trigger import *
import threading
import multiprocessing
import socket
import peewee
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

        self.rpc.log_info("Initializing database")
        db_success = False
        try:
            self.db = dbhandler(domosSettings.getDBConfig())
            self.db.connect()
        except peewee.ImproperlyConfigured as err:
            self.rpc.log_crit("Cannot use database connection: {}".format(err))
        except peewee.OperationalError as err:
            self.rpc.log_crit("Database connection error: {}".format(str(err)))
        if self.db.connected:
            self.db.create_tables()
            self.db.init_tables()
            self.rpc.log_info("Done initializing database")
            self.triggerchecker = triggerChecker(logger=self.rpc)
            self.triggerqueue = self.triggerchecker.getqueue()
            self.triggerchecker.start()
            self.actionhandler = actionhandler(self.rpc, logger=self.rpc)
            self.actionqueue = self.actionhandler.getqueue()
            self.triggerchecker.setactionqueue(self.actionqueue)
            self.actionhandler.start()
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
                    pprint.pprint(argslist)
                self.db.addRPC(module, rpc['key'], rpc['type'], argslist)
        else:
            self.rpc.log_debug("Sending sensors to module")
            return self.db.getModuleSensors(module)

    def addSensor(self, module_id=0, data=None, send=False):
        #add sensor to the database, if send is True, also send it to the module
        self.rpc.log_info('adding sensor from module {0} with ident {1}'.format(module_id, data['ident']))
        module = self.db.getModuleByID(module_id)
        sensor = self.db.addSensor(module, data['ident'], data)
        self.rpc.log_info('Sensor added')
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
            self.rpc.log_debug('logging trigger value for {0} with value {1}'.format(data['ident'],data['value']))
            self.db.addValue(data['key'], data['value'])
        except Exception as e:
            self.rpc.log_warn('Something went wrong registering trigger value for {0}: {1}'.format(data['ident'], e))
        else:
            #lauch trigger checks
            self.rpc.log_debug('posting sensordata to trigger processor')
            self.triggerqueue.put(("sensor", data['key'], data['value']))

    def addTrigger(self):
        pass

    def run(self):
        self.rpc.log_info("starting Dashi consumer")
        while not self.shutdown:
            self.rpc.listen()

    def end(self):
        self.shutdown = True

class domos:
    def __init__(self):
        pass

    def main(self):
        logger = rpclogger()
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
            sleep(1)
            #moduleargs = {'module_id': 1, 'send': True, 'data': {'ident': 'Sensortest', 'start.second': '*/20' }}
            #msgh.rpc.call('domoscore', 'addSensor', **moduleargs)
            msgh.join()

        #kwargs = {'key': 'test', 'ident': 'testMain', 'jobtype': 'Toggle', 'start': {'second': '10,30,50'}, 'stop': {'second':'0,20,40'}}

        
        #msgh.rpc.call('domoscore', 'sendAllSensors')
        #print(msgh.dashi.call('domosTime','getTimers'))
        #print(msgh.dashi.call('domoscore','getSensorValue', **{'sensor_id': 1}))
