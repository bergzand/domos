
from peewee import *
import datetime
import math
import statistics

dbconn = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']


class BaseModel(Model):
    translations=[('id','id')]
    class Meta:
        database = dbconn

    @classmethod
    def get_by_id(cls, num):
        return cls.get(cls.id == num)
    def to_dict(self,**kwargs):
        print(self.__class__)
        if issubclass(self.__class__,BaseModel):
            t = super(self.__class__,self).translations
        else:
            t = []
        print(t)
        t+=self.translations
        print(t)
        d={name: getattr(self,variable) for variable,name in t}
        return d
        
        
class Module(BaseModel):
    translations=[('name','name'),('queue','q'),('active','active'),('desc','des')]
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
    translations=[('rpctype','rpctype'),('desc','des')]
    rpctype = CharField()
    desc = TextField(null=True)


class ModuleRPC(BaseModel):
    translations=[('key','key'),('desc','des')]
    module = ForeignKeyField(Module, related_name='rpcs', on_delete='CASCADE')
    rpctype = ForeignKeyField(RPCType)
    key = CharField()
    desc = TextField(null=True)


class RPCArg(BaseModel):
    translations=[('name','name'),('rpcargtype','rpcargtype'),('optional','optional'),('desc','des')]
    modulerpc = ForeignKeyField(ModuleRPC, on_delete='CASCADE', related_name='args')
    name = CharField()
    rpcargtype = CharField()
    optional = BooleanField(default=False)
    desc = TextField(null=True)


class Sensor(BaseModel):
    translations=[('ident','name'),('active','active'),('instant','instant'),('desc','des')]
    module = ForeignKeyField(Module, related_name='sensors', on_delete='CASCADE')
    ident = CharField()
    active = BooleanField(default=True)
    instant = BooleanField(default=False)
    desc = TextField(null=True)


class SensorValue(BaseModel):
    translations=[('value','value'),('timestamp','timestamp'),('descr','des')]
    sensor = ForeignKeyField(Sensor, related_name='values', on_delete='CASCADE')
    value = CharField()
    timestamp = DateTimeField(default=datetime.datetime.now)
    descr = None

    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Sensor', 'Timestamp'), True)
            )


class SensorArg(BaseModel):
    translations=[('value','value')]
    sensor = ForeignKeyField(Sensor, related_name='args', on_delete='CASCADE')
    rpcarg = ForeignKeyField(RPCArg)
    value = CharField()


class Macro(BaseModel):
    translations=[('name','name'),('value','value')]
    name = CharField()
    value = CharField()


class Expression(BaseModel):
    translations=[('expression','expression')]
    expression = CharField()
    pickled = BlobField(null=True)


class Trigger(BaseModel):
    translations=[('name','name'),('record','record'),('lastvalue','lastvalue')]
    name = CharField()
    expression = ForeignKeyField(Expression)
    record = BooleanField()
    lastvalue = CharField(null=True, default="null")


class TriggerValue(BaseModel):
    translations=[('value','value'),('timestamp','timestamp')]
    trigger = ForeignKeyField(Trigger, related_name='values', on_delete='CASCADE')
    value = CharField()
    timestamp = DateTimeField(default=datetime.datetime.now)

    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Trigger', 'Timestamp'), True)
            )


class VarSensor(BaseModel):
    translations=[('function','function'),('args','args')]
    sensor = ForeignKeyField(Sensor, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class VarTrigger(BaseModel):
    translations=[('function','function'),('args','args')]
    trigger = ForeignKeyField(Trigger, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class Action(BaseModel):
    translations=[('ident','name')]
    module = ForeignKeyField(Module, related_name='actions')
    ident = CharField()


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
            if args:
                argdict = [{'name':name,
                            'RPCargtype': rpctype,
                            'Optional':opt,
                            'descr': descr,
                            'ModuleRPC': newrpc}for name, rpctype, opt, decr in args]
                RPCArg.insert_many(argdict).execute()

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

    def getModules(self):
        #returns an iterator with Module objects
        return Module.select().naive().iterator()

    def getModule(self, modulename):
        #returns Module object with modulename
        return Module.get(Module.name == modulename)

    def getModuleByID(self, id):
        return Module.get_by_id(id)

    def getRPCs(self, module, type):
        return ModuleRPC.select(ModuleRPC).join(RPCTypes).where((ModuleRPC.Module == module) & (RPCTypes.rpctype == type))
        
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
        return Sensors.select(Sensors, Module).join(Module).where(Sensors.Module == module)

    def getSensors(self):
        return Sensors.select(Sensors, Module).join(Module)

    def getSensorByIdent(self, ident):
        return Sensors.get(Sensors.ident == ident)
    
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

    def addValue(self, sensor_id, value):
        value = SensorValues.create(Sensor=sensor_id, Value=str(value))

    def getsensorfunctions(self, match_id):
        return SensorFunctions.select(SensorFunctions, Sensors).join(Sensors).where(SensorFunctions.Match == match_id)

    def gettriggerfunctions(self, match_id):
        return TriggerFunctions.select(TriggerFunctions, Triggers).join(Triggers).where(TriggerFunctions.Match == match_id)

    def gettriggersfromsensor(self, sensor_id):
        funcs = Triggers.select(Triggers, Match).join(Match).join(SensorFunctions, JOIN_INNER).where(SensorFunctions.Sensor == sensor_id).iterator()
        return [data for data in funcs]

    def gettriggersfromtrigger(self, trigger_id):
        funcs = Triggers.select(Triggers, Match).join(Match)\
            .join(TriggerFunctions)\
            .where(
                (TriggerFunctions.Trigger == trigger_id) &
                (Triggers.id != trigger_id)).iterator()
        return funcs
              
    def addTrigger(self, name, trigger, sensorlist, descr=None):
        '''
            trigger string examples:
            "__trig0__ == True"
        '''
        trigger = Triggers.create(Name=name, Trigger=trigger)
    
    def addTriggervalue(self, trigger, value):
        trigger.Lastvalue = value
        trigger.save()
        if trigger.Record:
            value = TriggerValues.create(Trigger=trigger, Value=value)
    
    def checkSensorTriggers(self, sensor):
        sensortriggers = Triggers.Select().Join(Functions).Where(Functions.Sensor == sensor)
        for sensortrigger in sensortriggers:
            pass
            #check this sensor on true

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
