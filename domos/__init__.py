from dashi import DashiConnection
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
from domos.util.domoslog import rpclogger
from domos.util.rpc import rpc
from domos.modules.domosTime import domosTime
import threading
import multiprocessing
import socket
import peewee
from time import sleep

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
        self.rpc.handle(self.activateAllSensors, "sendAllSensors")

        self.rpc.log_info("Initializing database")
        db_success = False
        try:
            init_dbconn(domosSettings.getDBConfig())
            self.db = db
            self.db.connect()
            db_success = True
        except peewee.ImproperlyConfigured as err:
            self.rpc.log_crit("Cannot use database connection: {}".format(err))
        except peewee.OperationalError as err:
            self.rpc.log_crit("Database connection error: {}".format(str(err)))
        if db_success:
            create_tables()
            init_tables()
            modules = Module.select()
            for module in modules:
                module.Active = True
                module.save()
            self.rpc.log_info("Done initializing database")
        else:
            self.shutdown = True

    def getSensorValue(self, sensor_id):
        sensor = Sensors.get_by_id(sensor_id)
        return sensor.last()


    def register(self, data=None):
        returnvalue = False
        try:
            module = Module.get(Module.name == data['name'])
            module.Activate = True
            module.save()
            self.rpc.log_info("Already registered module {} found, activated module".format(data['name']))
            allsensors = Sensors.select().where((Sensors.Active == True) & (Sensors.Module == module))
            sensorslist = []
            for sensor in allsensors:
                queue, key, kwargs = self.getSensor(sensor_id=sensor.id, ident=sensor.ident)
                sensorslist.append(kwargs)
            returnvalue = sensorslist
        except DoesNotExist:
            self.rpc.log_info('Registering {} module'.format(data['name']))
            module = Module()
            module.name = data['name']
            module.queue = data['queue']
            module.active = True
            module.save()
            for rpc in data['rpc']:
                newrpc = ModuleRPC()
                newrpc.Module = module
                newrpc.Key = rpc['key']
                newrpc.RPCType = RPCTypes.get(RPCTypes.rpctype == rpc['type'])
                newrpc.save()
                if "args" in rpc:
                    for arg in rpc['args']:
                        newarg = RPCArgs()
                        newarg.name = arg['name']
                        newarg.RPCargtype = arg['type']
                        newarg.ModuleRPC = newrpc
                        newarg.save()
            returnvalue = True
        return returnvalue

    def addSensor(self, module_id=0, data=None, send=False):
        #add sensor to the database, if send is True, also send it to the module
        self.rpc.log_info('adding sensor from module {0} with ident {1}'.format(module_id, data['ident']))
        sensor = Sensors()
        sensor.ident = data['ident']
        sensor.Module = Module.get_by_id(module_id)
        sensor.save()
        self.rpc.log_info('Sensor added')
        self.rpc.log_debug('getting arguments for sensor')
        rpcargs = RPCArgs.select().join(ModuleRPC).join(RPCTypes).where((ModuleRPC.Module == module_id) &
                                                                        (RPCTypes.rpctype == 'add'))
        argdata = []
        for rpcarg in rpcargs:
            if rpcarg.name in data:
                arg = {'Sensor': sensor, 'RPCArg': rpcarg, 'Value': data[rpcarg.name]}
                argdata.append(arg)
                #TODO: missing key exceptions and handling
        with self.db.transaction():
            SensorArgs.insert_many(argdata).execute()
        sensargs = SensorArgs.select().where(Sensors.id == sensor.id)
        if send:
            queue, key, kwargs = self.getSensor(sensor.id, sensor.ident)
            self.rpc.fire(queue, key, **kwargs)

    def getSensor(self, sensor_id=0, ident="desc"):
        #send sensor definition to the module
        self.rpc.log_debug("Sensor send function called")
        kwargs = dict()
        sensor = Sensors.get_by_id(sensor_id)

        def _getdict(kwarg, key, value):
            if len(key.split('.', 1)) > 1:
                start, end = key.split('.', 1)
                if start not in kwarg:
                    kwarg[start] = {}
                newdict = _getdict(kwarg[start], end, value)
                kwarg[start] = newdict
                return kwarg
            else:
                return {key: value}
        sensorargs = SensorArgs.select().where(SensorArgs.Sensor == sensor_id)
        kwargs = {}
        for sensorarg in sensorargs:
            value = sensorarg.Value
            rpcarg = RPCArgs.get(RPCArgs.id == sensorarg.RPCArg)
            kwargs = _getdict(kwargs, rpcarg.name, value)
        kwargs['key'] = sensor_id
        kwargs['ident'] = ident
        sendmod = sensor.Module
        queue = sendmod.queue
        rpctyp = RPCTypes.get(RPCTypes.rpctype == 'add')
        rpcdata = ModuleRPC.select().where((rpctyp.id == ModuleRPC.RPCType) &
                                          (ModuleRPC.Module == sendmod.id))
        for key in rpcdata:
            rpckey = key
        return queue, rpckey.Key, kwargs

    def sensorValue(self, data=None):
        #print(data)
        try:
            self.rpc.log_debug('logging trigger value for {0} with value {1}'.format(data['ident'],data['value']))
            sensor = Sensors.get_by_id(data['key'])
            value = SensorValues()
            value.Sensor = sensor
            value.Value = str(data['value'])
            value.save()
        except:
            self.rpc.log_warn('Something went wrong registering trigger value for {0}'.format(data['ident']))

    def activateAllSensors(self, module_id=None):
        #todo send only for active modules
        if module_id:
            allsensors = Sensors.select().join(Module).where(Sensors.Active == True & Module.id == module_id)
        else:
            self.rpc.log_info('Sending all activated sensors')
            allsensors = Sensors.select().join(Module).where(Sensors.Active == True & Module.Active == True)
        for sensor in allsensors:
            queue, key, kwargs = self.getSensor(sensor.id, sensor.ident)
            self.rpc.fire(queue, key, **kwargs)

    def run(self):
        self.rpc.log_info("starting Dashi consumer")
        while not self.shutdown:
            self.rpc.listen()

    def end(self):
        self.shutdown = True


class actionhandler:
    def __init__(self):
        pass


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
            msgh.join()

        #kwargs = {'key': 'test', 'ident': 'testMain', 'jobtype': 'Toggle', 'start': {'second': '10,30,50'}, 'stop': {'second':'0,20,40'}}

        #moduleargs = {'module_id': 1, 'send': True, 'data': {'ident': 'Sensortest', 'start.second': '*/10' }}
        #msgh.dashi.call('domoscore', 'addSensor', **moduleargs)
        #msgh.rpc.call('domoscore', 'sendAllSensors')
        #print(msgh.dashi.call('domosTime','getTimers'))
        #print(msgh.dashi.call('domoscore','getSensorValue', **{'sensor_id': 1}))
