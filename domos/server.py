import os
import importlib

import importlib.machinery
import domos.util.domoslog as domoslog
from domos.util.rpc import rpc
from domos.handlers import *
from domos.util.db import *
from collections import namedtuple

class MessageHandler(threading.Thread):
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
        self.rpc.handle(self.add_sensor, "add_sensor")
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
            self.triggerchecker = TriggerChecker(loghandler=rpchandle)
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
            module = Module.get_by_name(data['name'])

        except DoesNotExist:
            module = Module.add(name=data['name'], queue=data['queue'])
            for rpc in data['rpc']:
                argslist = []
                if "args" in rpc:
                    argslist = [(arg['name'],
                                 arg['type'],
                                 arg.get('optional', False),
                                 arg.get('descr', None)) for arg in rpc['args']]
                ModuleRPC.add(module, rpc['key'], rpc['type'], argslist)
        else:
            self.logger.debug("Sending sensors to module")
            sensors = Sensor.get_by_module(module)
            module.active = True
            module.save()
            sensorlist = []
            for sensor in sensors:
                sensordict = SensorArg.get_dict(sensor)
                sensordict['rpc'] = sensor.modulerpc.key
                sensorlist.append(sensordict)
            return sensorlist

    def add_sensor(self, module_id=0, data=None, send=False):
        # add sensor to the database, if send is True, also send it to the module
        self.logger.info('adding sensor from module {0} with ident {1}'.format(module_id, data['ident']))
        module = Module.get_by_id(module_id)
        sensor = Sensor.add(module, data['ident'], data)
        self.logger.info('Sensor added')
        if send:
            self.sendSensor(module, sensor)

    def sendSensor(self, module, sensor):
        rpccall = ModuleRPC.get_by_module(module, 'add')[0]
        self.rpc.call(module.queue,
                      rpccall.Key,
                      **self.db.getSensorDict(sensor))

    def sensorValue(self, key=None, value=None, timestamp=None):
        """RPC function to send a data value to the core. It also triggers the checking of depending triggers

        :param key: key of the sensor, also the primary key of the sensor in the database
        :param value: new value to send to the database
        :param timestamp: Timestamp of the value, leave empty to set it as the current time
        """

        try:
            self.logger.debug('logging trigger value for {0} with value {1}'.format(key, value))
            Sensor.get_by_id(key).add_value(value)
        except Exception as e:
            self.logger.warn('Something went wrong registering trigger value for {0}: {1}'.format(key, e))
        else:
            # lauch trigger checks
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
        # convert to list of tuples
        return [(sensor.ident,
                 sensor.Instant,
                 sensor.Active,
                 sensor.ModuleRPC.Module.name,
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

    module = namedtuple("module", ['name', 'module', 'process'])

    def __init__(self, args):
        self.args = args
        self.configfile = args.configfile
        self.modulelist = []
        ds.domosSettings.setConfigFile(self.configfile)
        self.settings = ds.domosSettings.get_core_config()

    def _get_files(self, mod_root_dir):
        self.logger.info("getting al modules from %s", mod_root_dir)
        #build a list of file in the hookdir
        for mod_name in os.listdir(mod_root_dir):
            mod_dir = os.path.join(mod_root_dir, mod_name)
            if os.path.isdir(mod_dir) and mod_name != "__pycache__":
                self.logger.debug("found mod mod_name: %s", mod_name)
                mod_file = mod_name +'.py'
                mod_path = os.path.join(mod_dir, mod_file)
                yield mod_path, mod_name

    def _module_loader(self, dir):
        working_dir = os.getcwd()
        print(working_dir)
        if type(dir) is list:
            for moddir in dir:
                if not os.path.isabs(moddir):
                    moddir = os.path.join(working_dir, moddir)
                for file, mod_name in self._get_files(moddir):
                    print("modfile is:", file)
                    #self.logger.debug("importing module %s" % mod)
                    mod = importlib.machinery.SourceFileLoader(mod_name, file)
                    module = mod.load_module()
                    module.start()
                    self.logger.info("Loaded module %s" % mod_name)
                    self.modulelist.append(domos.module(mod_name, module, None))

        #for module in self.modulelist:
        #    self.logger.info(dir(module.loaded_module))

    def main(self):
        logger = domoslog.rpclogger()
        logger.start()
        msgh = MessageHandler()
        if msgh.shutdown:
            self.logger.log_critical("Initialization error, shutting down", "domoscore")
        else:
            msgh.start()
            self.rpc = rpc('Main')
            rpchandle = domoslog.rpchandler(self.rpc)
            self.logger = logging.getLogger('Main')
            self.logger.addHandler(rpchandle)
            self.logger.setLevel(domosSettings.getLoggingLevel('core'))
            self._module_loader(self.settings['module_dir'])
            msgh.rpc.log_info('waiting for modules to register')
            msgh.join()


    @staticmethod
    def parsersettings(parser):

        parser.add_argument('--verbose', '-v', action='count',
                            help='Verbosity of the server')
        parser.add_argument('--daemon', '-d', action='store_true',
                            help='Verbosity of the server')
        return parser