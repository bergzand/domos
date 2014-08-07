
from peewee import *
import datetime



#db = SqliteDatabase('test.db', threadlocals = True)
dbconn = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']


class BaseModel(Model):
    descr = TextField(null=True)

    class Meta:
        database = dbconn

    @classmethod
    def get_by_id(cls, num):
        return cls.get(cls.id == num)


class Module(BaseModel):
    name = CharField()
    queue = CharField()
    Active = BooleanField()

class RPCTypes(BaseModel):
    rpctype = CharField()


class ModuleRPC(BaseModel):
    Module = ForeignKeyField(Module, on_delete='CASCADE')
    RPCType = ForeignKeyField(RPCTypes)
    Key = CharField()


class RPCArgs(BaseModel):
    ModuleRPC = ForeignKeyField(ModuleRPC, on_delete='CASCADE')
    name = CharField()
    RPCargtype = CharField()
    Optional = BooleanField(default=False)



class Sensors(BaseModel):
    Module = ForeignKeyField(Module, on_delete='CASCADE')
    ident = CharField()
    Active = BooleanField(default=True)
    
    def last(self, num=1):
        #returns the last - num value from the database
        selection = SensorValues.select().where(Sensors.id == self.id)
        return selection.limit(num).offset(num).dicts().get()

    def avg(self, num):
        #returns the average of the last num values
        selection = SensorValues.select().where(Sensors.id == self.id)
        return selection.dicts().limit(num).offset(num)['Value']


class SensorValues(BaseModel):
    Sensor = ForeignKeyField(Sensors, on_delete='CASCADE')
    Value = CharField()
    Timestamp = DateTimeField(default=datetime.datetime.now)
    descr = None

    class meta:
        order_by = ('-Timestamp',)


class SensorArgs(BaseModel):
    Sensor = ForeignKeyField(Sensors, on_delete='CASCADE')
    RPCArg = ForeignKeyField(RPCArgs)
    Value = CharField()

class Triggers(BaseModel):
    Name = CharField()
    

class SensorsForTrigger(BaseModel):
    Trigger = ForeignKeyField(Triggers)
    Sensor = ForeignKeyField(Sensors)
    

class Actions(BaseModel):
    Module = ForeignKeyField(Module)
    ident = CharField()


class ActionArgs(BaseModel):
    Action = ForeignKeyField(Actions)
    RPCArg = ForeignKeyField(RPCArgs)
    Value = CharField()


class ActionsForTrigger(BaseModel):
    Action = ForeignKeyField(Actions)
    Trigger = ForeignKeyField(Triggers)


class dbhandler:
    def __init__(self, conf):
        self.connected = False
        try:
            driver = conf.pop('driver')
        except:
            raise ImproperlyConfigured("No database driver found in config")
        try:
            database = conf.pop('database')
        except:
            raise ImproperlyConfigured("No database found in config")
        else:
            databaseconn = None
            if driver == 'mysql':
                databaseconn = MySQLDatabase(database, **conf)
            elif driver == 'postgres':
                databaseconn = PostgresqlDatabase(database, **conf)
            elif driver == 'sqlite':
                databaseconn == SqliteDatabase(database, **conf)
            else:
                raise ImproperlyConfigured("Cannot use database driver {}, only mysql, postgres and sqlite are supported".format(driver))
            if databaseconn:
                dbconn.initialize(databaseconn)
            else:
                raise ImproperlyConfigured("Cannot not initialize database connection")

    def create_tables(self):
        try:
            Module.create_table()
            RPCTypes.create_table()
            ModuleRPC.create_table()
            RPCArgs.create_table()
            Sensors.create_table()
            SensorValues.create_table()
            SensorArgs.create_table()
            Actions.create_table()
            ActionArgs.create_table()
        except InternalError:
            pass

    def init_tables(self):
        for type in rpctypes:
            try:
                RPCTypes.get(RPCTypes.rpctype == type)
            except RPCTypes.DoesNotExist:
                newtype = RPCTypes()
                newtype.rpctype = type
                newtype.save()
        q = Module.update(Active=False)
        q.execute()

    def connect(self):
        conn = dbconn.connect()
        self.connected = True
        return conn

    def close(self):
        conn = dbconn.close()
        self.connected = False
        return conn

    def addModule(self, name, queue, active=True):
        return Module.create(name=name, queue=queue, Active=active)

    def addRPC(self, module, key, rpctype, args, descr=None):
        '''
            Adds an rpc command to the database
            module: module object
            key: name of the rpc
            rpctype: string of the type of the rpc
            args: list of tuples, (name, type, optional, descr)
            descr: description of the rpc request
        '''

        rpcrecord = RPCTypes.get(RPCTypes.rpctype == rpctype)
        with dbconn.transaction():
            newrpc = ModuleRPC.create(Module=module, Key=key, RPCType=rpcrecord)
            argdict = []
            for name, rpctype, opt, decr in args:
                argdict.append({'name':name,
                                'RPCargtype': rpctype,
                                'Optional':opt,
                                'descr': descr,
                                'ModuleRPC': newrpc})
            RPCArgs.insert_many(argdict).execute()

    def _checkArg(self, oldArg, newArg):
        if oldArg == NewArg:
            return True
        else:
            return False
        
    def _checkRPC(self, rpc, args):
        pass
        
        

    def updateOrAddModule(self, moduledata):
        newmodule = False
        try:
            module = Module.get(Module.name == moduledata['name'])
            module.Active = True
            module.save()
        except DoesNotExist:
            module = Module()
            module.name = moduledata['name']
            module.queue = moduledata['queue']
            module.active = True
            module.save()
        else:
            newmodule = True
            with dbconn.transaction():
                for rpc in moduledata['rpc']:
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
        if not newmodule:
            pass

    def _getdict(self, kwarg, key, value):
        if len(key.split('.', 1)) > 1:
            start, end = key.split('.', 1)
            if start not in kwarg:
                kwarg[start] = {}
            newdict = self._getdict(kwarg[start], end, value)
            kwarg[start] = newdict
            return kwarg
        else:
            return {key: value}

    def getModule(self, modulename):
        return Module.get(Module.name == modulename)
    
    def getModuleByID(self, id):
        return Module.get_by_id(id)

    def getRPCCall(self, module, type):
        return ModuleRPC.select().join(RPCTypes).where((ModuleRPC.Module == module) & (RPCTypes.rpctype == type)).limit(1)[0]

    def addSensor(self, module, identifier, argdata):
        '''
            argdata: list of dicts with name=value pairs
        '''
        sensor = Sensors()
        sensor.ident = identifier
        sensor.Module = module
        sensor.save()
        rpcargs = RPCArgs.select().join(ModuleRPC).join(RPCTypes).where((ModuleRPC.Module == module) &
                                                                        (RPCTypes.rpctype == 'add'))
        queryargs = []
        for rpcarg in rpcargs:
            if rpcarg.name in argdata:
                arg = {'Sensor': sensor, 'RPCArg': rpcarg, 'Value': argdata[rpcarg.name]}
                queryargs.append(arg)
                #TODO: missing key exceptions and handling
        q = SensorArgs.insert_many(queryargs).execute()
        return sensor

    def getModuleSensors(self, module):
        sensors = Sensors.select().join(SensorArgs).join(RPCArgs).where(Sensors.Module == module)
        return [self.getSensorDict(sensor) for sensor in sensors]

    def getSensorDict(self, sensor):
        kwargs = dict()
        sensorargs = SensorArgs.select().where(SensorArgs.Sensor == sensor)
        kwargs = {}
        for sensorarg in sensorargs:
            value = sensorarg.Value
            rpcarg = sensorarg.RPCArg
            kwargs = self._getdict(kwargs, rpcarg.name, value)
        kwargs['key'] = sensor.id
        kwargs['ident'] = sensor.ident
        return kwargs

    def addValue(self, sensor_id, value):
        value = SensorValues.create(Sensor=sensor_id, Value=str(value))
