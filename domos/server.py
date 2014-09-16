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
        """Create the core messagehandler. Also starts up depending threads
        """ 
        threading.Thread.__init__(self)
        self.shutdown = False
        self.name = 'domoscore'
        self.rpc = rpc(self.name)
        self.rpc.log_info("starting main thread")
        self.rpc.handle(self.register, "register")
        self.rpc.handle(self.sensorValue, "sensorValue")
        self.rpc.handle(self.addSensor, "addSensor")
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
        

    def register(self, data=None):
        """RPC function to register a module

        :param data: dictionary with the module data containing the name, queue and rpc's of the module 
        """
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
            sensors = self.db.getModuleSensors(module)
            return [self.db.getSensorDict(sensor) for sensor in sensors]

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

    def sensorValue(self, key=None, value=None, timestamp=None):
        """RPC function to send a data value to the core. It also triggers the checking of depending triggers

        :param key: key of the sensor, also the primary key of the sensor in the database
        :param value: new value to send to the database
        """

        try:
            self.logger.debug('logging trigger value for {0} with value {1}'.format(key,value))
            self.db.addValue(key, value)
        except Exception as e:
            self.logger.warn('Something went wrong registering trigger value for {0}: {1}'.format(key, e))
        else:
            #lauch trigger checks
            self.logger.debug('posting sensordata to trigger processor')
            self.triggerqueue.put(("sensor", key, value))

    def run(self):
        """Start the core thread
        """
        self.logger.info("starting Dashi consumer")
        while not self.shutdown:
            self.rpc.listen()

    def end(self):
        """Stop the core thread
        """
        self.shutdown = True


class apihandler(threading.Thread):

    def __init__(self):
        """Create a api handler. This is automaticaly build from the messagehandler
        """
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
        self.rpc.handle(self.listSensors, "getSensors")
        self.rpc.handle(self.listSensorArgs, "getArgs")
        self.rpc.handle(self.listPrototypes, "getProtos")
        self.db = dbhandler()

    def listModules(self):
        """RPC api call, returns a list with a tuple with module data
           (name, queue, Active)

        """
        modules = [(module.name,
                    module.queue,
                    module.Active) for module in self.db.getModules()]
        return modules

    def listSensors(self, module=None):
        """RPC call, returns a list of tuples with sensors database
           (identifier, instant, activated, module name, description)
        
        :param module: If a module is specified, it returns only sensors
        of that module
        """
        if module:
            try:
                sensors = self.db.getModuleSensors(self.db.getModule(module))
            except DoesNotExist:
                return None
        else:
            sensors = self.db.getSensors()
        #convert to list of tuples
        return [(sensor.ident,
                 sensor.Instant,
                 sensor.Active,
                 sensor.Module.name,
                 sensor.descr) for sensor in sensors]

    def listSensorArgs(self, sensor=None):
        """RPC api call, lists all arguments of a sensor
        
        :param: sensor: name of the sensor to query
        """
        returnvalue = None
        if not sensor:
            pass
        else:
            sensor = self.db.getSensorByIdent(sensor)
            returnvalue = self.db.getSensorDict(sensor)
        return returnvalue

    def listPrototypes(self, module=None):
        """returns a list of a 2 value tuple, key, arguments. Arguments
        are a list of arguments, each containing a list containing the
        name, type, optionality and description
        
        :param module: Name of the module to query
        """
        returnvalue = None
        try:
            module = self.db.getModule(module)
        except DoesNotExist:
            returnvalue = None
        else:
            rpcs = self.db.getRPCs(module, 'add')
            returnvalue = [(rpc.Key, [(arg.name,
                                       arg.RPCargtype,
                                       arg.Optional,
                                       arg.descr) for arg in rpc.args]) for rpc in rpcs]
        return returnvalue

    def run(self):
        """Start the api handler thread
        """
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
    @staticmethod
    def parsersettings(parser):
        
        parser.add_argument('--verbose', '-v', action='count',
                              help='Verbosity of the server')
        parser.add_argument('--daemon', '-d', action='store_true',
                              help='Verbosity of the server')
        return parser