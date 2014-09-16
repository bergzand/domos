
from peewee import *
import datetime
import math
import statistics

dbconn = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']


class BaseModel(Model):

    class Meta:
        database = dbconn

    @classmethod
    def get_by_id(cls, num):
        return cls.get(cls.id == num)

    def jsjson(self):
        return {'id': self.id, 'descr': self.descr}


class Module(BaseModel):
    name = CharField()
    queue = CharField()
    active = BooleanField()
    desc = TextField(null=True)

    @classmethod
    def add(cls, name, queue, active=True):
        return cls.create(name=name, queue=queue, Active=active)

    @classmethod
    def list(cls):
        #returns a list of modules
        return [module for module in cls.getModules()]

    @classmethod
    def get_by_name(cls, name):
        return cls.get(Module.name == name)


class RPCType(BaseModel):
    rpctype = CharField()
    desc = TextField(null=True)


class ModuleRPC(BaseModel):
    module = ForeignKeyField(Module, related_name='rpcs', on_delete='CASCADE')
    rpctype = ForeignKeyField(RPCType)
    key = CharField()
    desc = TextField(null=True)

    @classmethod
    def add(cls, module, key, rpctype, args, desc=None):
        """
            Adds an rpc command to the database
            
            :param module: module object
            :param key: name of the rpc
            :param rpctype: string of the type of the rpc
            :param args: list of tuples, (name, type, optional, descr)
            :param descr: description of the rpc request
        """

        rpcrecord = RPCType.get(RPCType.rpctype == rpctype)
        with dbconn.transaction():
            newrpc = ModuleRPC.create(Module=module, Key=key, RPCType=rpcrecord)
            if args:
                argdict = [{'name': name,
                            'RPCargtype': rpctype,
                            'Optional': opt,
                            'desc': desc,
                            'ModuleRPC': newrpc}for name, rpctype, opt, decr in args]
                RPCArg.insert_many(argdict).execute()


    @classmethod
    def get_by_module(cls, module, type=None):
        """
            Retrieve RPC's associated with a module
            
            :param module: :class`Module` to retrieve remote procedures for
            :param type: type of rpc to return
            :rtype Iterator with remote procedures
        """
        rtrn = None
        if type:
            rtrn = ModuleRPC.select(ModuleRPC).join(RPCType).where((ModuleRPC.Module == module) & (RPCType.rpctype == type))
        else:
            rtrn = ModuleRPC.select(ModuleRPC).join(RPCType).where((ModuleRPC.Module == module) & (RPCType.rpctype == type))
        return rtrn

class RPCArg(BaseModel):
    modulerpc = ForeignKeyField(ModuleRPC, on_delete='CASCADE', related_name='args')
    name = CharField()
    rpcargtype = CharField()
    optional = BooleanField(default=False)
    desc = TextField(null=True)

    @classmethod
    def get_by_type(cls, module, type):
        return RPCArg.select().join(ModuleRPC).join(RPCType).where((ModuleRPC.Module == module) &
                                                                   (RPCType.rpctype == 'add'))


class Sensor(BaseModel):
    module = ForeignKeyField(Module, related_name='sensors', on_delete='CASCADE')
    name = CharField()
    active = BooleanField(default=True)
    instant = BooleanField(default=False)
    desc = TextField(null=True)

    @classmethod
    def add(cls, module, name, argdata):
        """
            Add a sensor to the database

            :param argdata: list of dicts with name=value pairs
        """
        sensor = Sensor()
        sensor.name = name
        sensor.Module = module
        sensor.save()
        rpcargs = RPCArg.get_by_type(module, type)
        queryargs = []
        for rpcarg in rpcargs:
            if rpcarg.name in argdata:
                arg = {'Sensor': sensor, 'RPCArg': rpcarg, 'Value': argdata[rpcarg.name]}
                queryargs.append(arg)
                #TODO: missing key exceptions and handling
        q = SensorArg.insert_many(queryargs).execute()
        return sensor

    @classmethod
    def get_by_module(cls, module):
        return Sensor.select(Sensor, Module).join(Module).where(Sensor.Module == module)

    @classmethod
    def get_by_name(cls, name):
        return Sensor.get(Sensor.name == name)

    def add_value(self, value):
        value = SensorValue.create(sensor=self, value=str(value))


class SensorValue(BaseModel):
    sensor = ForeignKeyField(Sensor, related_name='values', on_delete='CASCADE')
    value = CharField()
    timestamp = DateTimeField(default=datetime.datetime.now)
    
    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Sensor', 'Timestamp'), True)
            )


class SensorArg(BaseModel):
    sensor = ForeignKeyField(Sensor, related_name='args', on_delete='CASCADE')
    rpcarg = ForeignKeyField(RPCArg)
    value = CharField()


class Macro(BaseModel):
    name = CharField()
    value = CharField()


class Expression(BaseModel):
    expression = CharField()
    pickled = BlobField(null=True)

    def get_used_sensors(self):
        return VarSensor.select(VarSensor, Sensor).join(Sensor).where(VarSensor == self)

    def get_used_triggers(self):
        return VarTrigger.select(VarTrigger, Trigger).join(VarTrigger).where(VarTrigger.expression == self)


class Trigger(BaseModel):
    name = CharField()
    expression = ForeignKeyField(Expression)
    record = BooleanField()
    lastvalue = CharField(null=True, default="null")

    def get_affected_triggers(self):
        return Trigger.select(Trigger, Expression).join(Expression)\
            .join(VarTrigger)\
            .where(
                (VarTrigger.trigger == self) &
                (Trigger != self)
                )

    def add_value(self, value):
        self.lastvalue = value
        self.save()
        if self.record:
            value = TriggerValue.create(trigger=self, value=value)

class TriggerValue(BaseModel):
    trigger = ForeignKeyField(Trigger, related_name='values', on_delete='CASCADE')
    value = CharField()
    timestamp = DateTimeField(default=datetime.datetime.now)

    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Trigger', 'Timestamp'), True)
            )


class VarSensor(BaseModel):
    sensor = ForeignKeyField(Sensor, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class VarTrigger(BaseModel):
    trigger = ForeignKeyField(Trigger, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class Action(BaseModel):
    module = ForeignKeyField(Module, related_name='actions')
    name = CharField()


class ActionArg(BaseModel):
    action = ForeignKeyField(Action, related_name='args')
    rpcarg = ForeignKeyField(RPCArg)
    value = ForeignKeyField(Expression)


class TriggerAction(BaseModel):
    #mapping of triggers and actions
    action = ForeignKeyField(Action, related_name='triggers')
    trigger = ForeignKeyField(Trigger, related_name='actions')
    expression = ForeignKeyField(Expression)


class dbhandler:
    def __init__(self, conf=None, database=None):
        self.connected = False
        if database:
            self.databaseconn = database
        elif conf:
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
                    databaseconn = MySQLDatabase(database, threadlocals=True, **conf)
                elif driver == 'postgres':
                    databaseconn = PostgresqlDatabase(database, threadlocals=True, **conf)
                elif driver == 'sqlite':
                    databaseconn = SqliteDatabase(database, threadlocals=True, **conf)
                else:
                    raise ImproperlyConfigured("Cannot use database driver {}, only mysql, postgres and sqlite are supported".format(driver))
                if databaseconn:
                    dbconn.initialize(databaseconn)
                else:
                    raise ImproperlyConfigured("Cannot not initialize database connection")

    def create_tables(self):
        tables = [Module,
                  RPCType,
                  ModuleRPC,
                  RPCArg,
                  Expression,
                  Sensor,
                  SensorValue,
                  SensorArg,
                  Action,
                  Trigger,
                  TriggerValue,
                  VarSensor,
                  VarTrigger,
                  TriggerAction,
                  ActionArg]

        for table in tables:
            try:
                table.create_table()
                print("created table:", table)
            except InternalError:
                pass

    def init_tables(self):
        for type in rpctypes:
            try:
                RPCType.get(RPCType.rpctype == type)
            except RPCType.DoesNotExist:
                newtype = RPCType()
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

    def getdict(self, kwarg, key, value):
        if len(key.split('.', 1)) > 1:
            start, end = key.split('.', 1)
            if start not in kwarg:
                kwarg[start] = {}
            key, value = self.getdict(kwarg[start], end, value)
            kwarg[start][key] = value
            print(kwarg)
            return start, kwarg[start]
        else:
            return key, value

    
    def getSensorDict(self, sensor):
        sensorargs = SensorArgs.select().where(SensorArgs.Sensor == sensor)
        kwargs = {}
        for sensorarg in sensorargs:
            value = sensorarg.Value
            rpcarg = sensorarg.RPCArg
            key, value = self.getdict(kwargs, rpcarg.name, value)
            kwargs[key] = value
        kwargs['key'] = sensor.id
        kwargs['ident'] = sensor.ident
        return kwargs

    def getActions(self, action):
        actionargs = ActionArgs.select(ActionArgs, RPCArgs).join(RPCArgs).where(ActionArgs.Action == action)
        return actionargs

    def gettriggersfromsensor(self, sensor_id):
        funcs = Triggers.select(Triggers, Match).join(Match).join(SensorFunctions, JOIN_INNER).where(SensorFunctions.Sensor == sensor_id).iterator()
        return [data for data in funcs]


    def getActionsfromtrigger(self, trigger_id):
        actions = ActionsForTrigger.select(ActionsForTrigger, Actions).join(Actions).where(ActionsForTrigger.Trigger == trigger_id)
        return [action for action in actions]


class sensorops:
    
    @staticmethod
    def operation(sensorfunction):
        op = {
        'last': sensorops.last,
        'avg': sensorops.avg,
        'sum': sensorops.sumation,
        'diff': sensorops.diff,
        'tdiff': sensorops.tdiff,
        }[sensorfunction.Function]
        print('looking up value with')
        return op(sensorfunction.Sensor, sensorfunction.Args)

    @staticmethod
    def _lastrecords(sensor, num):
        if sensor.Instant:
            return []
        else:
            return SensorValues.select().where(SensorValues.Sensor == sensor).order_by(SensorValues.Timestamp.desc()).limit(num).naive()

    @staticmethod
    def last(sensor, num):
        last = sensorops._lastrecords(sensor, int(num))
        if last:
            return last.select().offset(num).dicts().get()['Value']
        else:
            return 0

    @staticmethod
    def sumation(sensor, num):
        selection = sensorops._lastrecords(sensor, num)
        if last:
            result = math.fsum((int(i.Value) for i in selection))
            return result
        else:
            return 0

    @staticmethod
    def avg(sensor, num):
        selection = sensorops._lastrecords(sensor, num)
        if last:
            result = statistics.mean((int(i.Value) for i in selection))
            return result
        else:
            return 0

    @staticmethod
    def diff(sensor, args):
        selection = sensorops._lastrecords(sensor, 2)
        if selection:
            try:
                result1 = float(selection[0].Value)
                result2 = float(selection[1].Value)
            except:
                raise sensorerror(sensor, 'could not convert values to floating point numbers')
            result = result1 - result2
        else:
            result = 0
        return result

    def tdiff(sensor, args):
        selection = sensorops._lastrecords(sensor, 2)
        if selection:
            result1 = selection[0]
            result2 = selection[1]
            print(type(result1.Timestamp))
            result = (int(result1.Value) - int(result2.Value))/(result1.Timestamp - result2.Timestamp).total_seconds()
        else:
            result = 0
        return result

class triggerops:

    @staticmethod
    def operation(triggerfunction):
        op = {'last': triggerops.last,
              #'avg':  triggerops.avg,
              #'sum': triggerops.sumation,
              #'diff': triggerops.diff,
              #'tdiff': triggerops.tdiff,
              }[triggerfunction.Function]
        print('looking up value with')
        return op(triggerfunction.Trigger, triggerfunction.Args)

    @staticmethod
    def _lastrecords(trigger, num):
        return TriggerValues.select().where(TriggerValues.Trigger == trigger).order_by(TriggerValues.Timestamp.desc()).limit(num).naive()

    @staticmethod
    def last(trigger, num):
        last = triggerops._lastrecords(trigger, int(num))
        if last:
            return last.select().offset(num).dicts().get()['Value']
        else:
            return 0
    

class sensorerror(Exception):                                    
    def __init__(self, sensor, message):
        self.sensor = sensor
        self.message = message
