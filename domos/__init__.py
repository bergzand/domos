from dashi import DashiConnection
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
from domos.util.domoslog import rpclogger
from domos.modules.domosTime import domosTime
import threading
import multiprocessing
import socket
from time import sleep

from domos.util.db import *


class messagehandler(multiprocessing.Process):

    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.done = False
        self.name = 'domoscore'
        dashiconfig = domosSettings.getDashiConfig()
        self.dashi = DashiConnection(self.name, dashiconfig["amqp_uri"], dashiconfig['exchange'], sysname = dashiconfig['sysname'])
        self.logmsg("info", "starting main thread")
        self.dashi.handle(self.register, "register")
        self.dashi.handle(self.sensorValue, "sensorValue")
        self.dashi.handle(self.addSensor, "addSensor")
        self.dashi.handle(self.getSensorValue, "getSensorValue")
        self.dashi.handle(self.activateAllSensors, "sendAllSensors")

        self.logmsg("info", "Initializing database")
        
        self.db = db
        self.db.init("domos",**domosSettings.getDBConfig())
        self.db.connect()
        create_tables()
        init_tables()
        
        modules = Module.select()
        for module in modules:
            module.Active = True
            module.save()
        self.logmsg("info", "Done initializing database")
    

    def getSensorValue(self, sensor_id):
        sensor = Sensors.get_by_id(sensor_id)
        return sensor.last()

    def logmsg(self, level, msg):
        call = 'log_{}'.format(level)
        self.dashi.fire('log', 'log_debug', msg=msg, handle='core' )

    def register(self, data=None):
        try:
            module = Module.get(Module.name == data['name'])
            module.Activate = True
            module.save()
            self.logmsg("info", "Already registered module {} found, activated module".format(data['name']))
        except DoesNotExist:
            self.logmsg('info', 'Registering {} module'.format(data['name']))
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
        return True

    def addSensor(self, module_id=0, data=None, send=False):
        #add sensor to the database, if send is True, also send it to the module
        self.logmsg('info', 'adding sensor from module {0} with ident {1}'.format(module_id, data['ident']))
        sensor = Sensors()
        sensor.ident = data['ident']
        sensor.Module = Module.get_by_id(module_id)
        sensor.save()
        self.logmsg('info', 'Sensor added')
        self.logmsg('debug','getting arguments for sensor')
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
            self.sendSensor(sensor.id, sensor.ident)

    def sendSensor(self, sensor_id=0, ident="desc"):
        #send sensor definition to the module
        self.logmsg("Debug", "Sensor send function called")
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
        sensorargs = SensorArgs.select().where(Sensors.id == sensor_id)
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
        self.dashi.call(queue, rpckey.Key, **kwargs )

    def sensorValue(self, data=None):
        #print(data)
        try:
            self.logmsg('debug', 'logging trigger value for {0} with value {1}'.format(data['ident'],data['value']))
            sensor = Sensors.get_by_id(data['key'])
            value = SensorValues()
            value.Sensor = sensor
            value.Value = str(data['value'])
            value.save()
        except:
            self.logmsg('warn', 'Something went wrong registering trigger value for {0}'.format(data['ident']))

    def activateAllSensors(self, module_id=None):
        #todo send only for active modules
        if module_id:
            allsensors = Sensors.select().join(Module).where(Sensors.Active == True & Module.id == module_id)
        else:
            self.logmsg('info','Sending all activated sensors')
            allsensors = Sensors.select().join(Module).where(Sensors.Active == True & Module.Active == True)
        for sensor in allsensors:
            self.sendSensor(sensor.id, sensor.ident)

    def run(self):
        self.logmsg("info", "starting Dashi consumer")
        while not self.done:
            try:
                self.dashi.consume(timeout=2)
            except socket.timeout as ex:
                pass

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
        msgh.start()
        dt = domosTime()
        dt.start()
        msgh.logmsg("info",'waiting for modules to register')
        sleep(1)

        #kwargs = {'key': 'test', 'ident': 'testMain', 'jobtype': 'Toggle', 'start': {'second': '10,30,50'}, 'stop': {'second':'0,20,40'}}

        #moduleargs = {'module_id': 1, 'send': True, 'data': {'ident': 'Sensortest', 'start.second': '*/10' }}
        #msgh.dashi.call('domoscore', 'addSensor', **moduleargs)
        msgh.dashi.call('domoscore', 'sendAllSensors')
        #print(msgh.dashi.call('domosTime','getTimers'))
        #print(msgh.dashi.call('domoscore','getSensorValue', **{'sensor_id': 1}))

        msgh.join()
