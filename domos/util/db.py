
from peewee import *
import datetime
import math
import statistics

dbconn = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']


class BaseModel(Model):

    translations = [('id', 'id')]

    class Meta:
        database = dbconn

    @classmethod
    def get_by_id(cls, num):
        return cls.get(cls.id == num)

    def to_dict(self, **kwargs):
        print(self.__class__)
        if issubclass(self.__class__, BaseModel):
            t = super(self.__class__, self).translations
        else:
            t = []
        print(t)
        t += self.translations
        print(t)
        d = {name: getattr(self, variable) for variable, name in t}
        return d


class Module(BaseModel):
    translations = [('name', 'name'),
                    ('queue', 'q'),
                    ('active', 'active'),
                    ('desc', 'des')]
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
    translations = [('rpctype', 'rpctype'),
                    ('desc', 'des')]
    rpctype = CharField()
    desc = TextField(null=True)


class ModuleRPC(BaseModel):
    translations = [('key', 'key'),
                    ('desc', 'des')]
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
            newrpc = ModuleRPC.create(module=module, key=key, rpctype=rpcrecord)
            if args:
                argdict = [{'name': name,
                            'rpcargtype': rpctype,
                            'optional': opt,
                            'desc': desc,
                            'modulerpc': newrpc}for name, rpctype, opt, decr in args]
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
            rtrn = cls.select(cls).join(RPCType).where((cls.module == module) & (RPCType.rpctype == type))
        else:
            rtrn = cls.select(cls).join(RPCType).where((cls.module == module) & (RPCType.rpctype == type))
        return rtrn


class RPCArg(BaseModel):
    translations = [('name', 'name'),
                    ('rpcargtype', 'rpcargtype'),
                    ('optional', 'optional'),
                    ('desc', 'des')]
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
    translations = [('name', 'name'),
                    ('active', 'active'),
                    ('instant', 'instant'),
                    ('desc', 'des')]
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
        sensor.module = module
        sensor.save()
        rpcargs = RPCArg.get_by_type(module, type)
        queryargs = []
        for rpcarg in rpcargs:
            if rpcarg.name in argdata:
                arg = {'sensor': sensor, 'rpcarg': rpcarg, 'value': argdata[rpcarg.name]}
                queryargs.append(arg)
                #TODO: missing key exceptions and handling
        q = SensorArg.insert_many(queryargs).execute()
        return sensor

    @classmethod
    def get_by_module(cls, module):
        return Sensor.select(Sensor, Module).join(Module).where(Sensor.module == module)

    @classmethod
    def get_by_name(cls, name):
        return Sensor.get(Sensor.name == name)

    def add_value(self, value):
        value = SensorValue.create(sensor=self, value=str(value))

    def lastrecords(self, num):
        if self.instant:
            return []
        else:
            return SensorValue.select().where(SensorValue.sensor == self).order_by(SensorValue.timestamp.desc()).limit(num).naive()


class SensorValue(BaseModel):
    translations = [('value', 'value'),
                    ('timestamp', 'timestamp'),
                    ('descr', 'des')]
    sensor = ForeignKeyField(Sensor, related_name='values', on_delete='CASCADE')
    value = CharField()
    timestamp = DateTimeField(default=datetime.datetime.now)


    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Sensor', 'Timestamp'), True)
            )


class SensorArg(BaseModel):
    translations = [('value', 'value')]
    sensor = ForeignKeyField(Sensor, related_name='args', on_delete='CASCADE')
    rpcarg = ForeignKeyField(RPCArg)
    value = CharField()

    @staticmethod
    def _to_dict(kwarg, key, value):
        if len(key.split('.', 1)) > 1:
            start, end = key.split('.', 1)
            if start not in kwarg:
                kwarg[start] = {}
            key, value = SensorArg._to_dict(kwarg[start], end, value)
            kwarg[start][key] = value
            print(kwarg)
            return start, kwarg[start]
        else:
            return key, value

    @staticmethod
    def get_dict(sensor):
        sensorargs = SensorArg.select().where(SensorArg.sensor == sensor)
        kwargs = {}
        for sensorarg in sensorargs:
            value = sensorarg.value
            rpcarg = sensorarg.rpcarg
            key, value = SensorArg._to_dict(kwargs, rpcarg.name, value)
            kwargs[key] = value
        kwargs['key'] = sensor.id
        kwargs['name'] = sensor.name
        return kwargs


class Macro(BaseModel):
    translations = [('name', 'name'), ('value', 'value')]
    name = CharField()
    value = CharField()


class Expression(BaseModel):
    translations = [('expression', 'expression')]
    expression = CharField()
    pickled = BlobField(null=True)

    def get_used_sensors(self):
        return VarSensor.select(VarSensor, Sensor).join(Sensor).where(VarSensor.expression == self)

    def get_used_triggers(self):
        return VarTrigger.select(VarTrigger, Trigger).join(Trigger).where(VarTrigger.expression == self)


class Trigger(BaseModel):
    translations = [('name', 'name'),
                    ('record', 'record'),
                    ('lastvalue', 'lastvalue')]
    name = CharField()
    expression = ForeignKeyField(Expression)
    record = BooleanField()
    lastvalue = CharField(null=True, default="null")

    def get_affected_triggers(self):
        return Trigger.select(Trigger, Expression).join(Expression)\
            .join(VarTrigger)\
            .where(
                (VarTrigger.source == self) &
                (Trigger != self)
                )

    @classmethod
    def get_affected_by_trigger(cls, trigger_id):
        return Trigger.select(Trigger, Expression).join(Expression)\
            .join(VarTrigger)\
            .where(
                (VarTrigger.source == trigger_id) &
                (Trigger.id != trigger_id)
                )

    @classmethod
    def get_affected_by_sensor(cls, sensor_id):
        return Trigger.select(Trigger, Expression).join(Expression).join(VarSensor, JOIN_INNER).where(VarSensor.source == sensor_id).iterator()

    def add_value(self, value):
        self.lastvalue = value
        self.save()
        if self.record:
            value = TriggerValue.create(trigger=self, value=value)

    def lastrecords(self, num):
        rtn = 0
        if self.lastvalue:
            if self.record:
                rtn = TriggerValue.select().where(TriggerValue.trigger == self).order_by(TriggerValue.timestamp.desc()).limit(num).naive()
            else:
                rtn = self.lastvalue
        return rtn
            
        
class TriggerValue(BaseModel):
    translations = [('value', 'value'),
                    ('timestamp', 'timestamp')]
    trigger = ForeignKeyField(Trigger, related_name='values', on_delete='CASCADE')
    value = CharField()
    timestamp = DateTimeField(default=datetime.datetime.now)

    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Trigger', 'Timestamp'), True)
            )


class VarSensor(BaseModel):
    translations = [('function', 'function'),
                    ('args', 'args')]
    source = ForeignKeyField(Sensor, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class VarTrigger(BaseModel):
    translations = [('function', 'function'),
                    ('args', 'args')]
    source = ForeignKeyField(Trigger, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class Action(BaseModel):
    translations = [('name', 'name')]
    module = ForeignKeyField(Module, related_name='actions')
    name = CharField()


class ActionArg(BaseModel):
    action = ForeignKeyField(Action, related_name='args')
    rpcarg = ForeignKeyField(RPCArg)
    value = ForeignKeyField(Expression)

    @staticmethod
    def _to_dict(kwarg, key, value):
        if len(key.split('.', 1)) > 1:
            start, end = key.split('.', 1)
            if start not in kwarg:
                kwarg[start] = {}
            key, value = SensorArg._to_dict(kwarg[start], end, value)
            kwarg[start][key] = value
            print(kwarg)
            return start, kwarg[start]
        else:
            return key, value

    @classmethod
    def get_dict(cls, action, calculator):
        actionargs = cls.select().where(cls.action == action)
        kwargs = {}
        for act in actionargs:
            value = calculator.resolve(act.value)
            rpcarg = act.rpcarg
            key, value = cls._to_dict(kwargs, rpcarg.name, value)
            kwargs[key] = value
        return kwargs

class TriggerAction(BaseModel):
    #mapping of triggers and actions
    action = ForeignKeyField(Action, related_name='triggers')
    trigger = ForeignKeyField(Trigger, related_name='actions')
    expression = ForeignKeyField(Expression)
    
    @classmethod
    def get_by_trigger(cls, trigger):
        return cls.select(cls, Action).join(Action).where(cls.trigger == trigger)


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
        q = Module.update(active=False)
        q.execute()

    def connect(self):
        conn = dbconn.connect()
        self.connected = True
        return conn

    def close(self):
        conn = dbconn.close()
        self.connected = False
        return conn


class ops:

    @staticmethod
    def operation(function):
        op = {'last': ops.last,
              'avg': ops.avg,
              'sum': ops.sumation,
              'diff': ops.diff,
              'tdiff': ops.tdiff,
              }[function.function]
        print('looking up value with')
        return op(function.source, function.args)

    @staticmethod
    def last(source, num):
        last = source.lastrecords(int(num))
        if last:
            return last.select().offset(num).dicts().get()['value']
        else:
            return 0

    @staticmethod
    def sumation(source, num):
        selection = source.lastrecords(int(num))
        if selection:
            result = math.fsum((int(i.Value) for i in selection))
            return result
        else:
            return 0

    @staticmethod
    def avg(source, num):
        selection = source.lastrecords(int(num))
        if last:
            result = statistics.mean((int(i.Value) for i in selection))
            return result
        else:
            return 0

    @staticmethod
    def diff(source, args):
        selection = source.lastrecords(2)
        if selection:
            try:
                result1 = float(selection[0].Value)
                result2 = float(selection[1].Value)
            except:
                raise sensorerror(source, 'could not convert values to floating point numbers')
            result = result1 - result2
        else:
            result = 0
        return result

    @staticmethod
    def tdiff(source, args):
        selection = source.lastrecords(2)
        if selection:
            result1 = selection[0]
            result2 = selection[1]
            print(type(result1.Timestamp))
            result = (int(result1.value) - int(result2.value))/(result1.timestamp - result2.timestamp).total_seconds()
        else:
            result = 0
        return result


class sensorerror(Exception):
    def __init__(self, sensor, message):
        self.sensor = sensor
        self.message = message
