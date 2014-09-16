
from peewee import *
import datetime
import math
import statistics

dbconn = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']


class BaseModel(Model):
    descr = TextField(null=True)

    class Meta:
        database = dbconn

    @classmethod
    def get_by_id(cls, num):
        return cls.get(cls.id == num)
    def jsjson(self):
        return {'id':self.id,'descr':self.descr}

class Module(BaseModel):
    name = CharField()
    queue = CharField()
    Active = BooleanField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'name':self.name,'Active':self.Active,'queue':self.queue}
        )


class RPCTypes(BaseModel):
    rpctype = CharField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'rpctype':self.rpctype}
        )

class ModuleRPC(BaseModel):
    Module = ForeignKeyField(Module, on_delete='CASCADE')
    RPCType = ForeignKeyField(RPCTypes)
    Key = CharField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Module':self.Module,'RPCType':self.RPCType,'Key':self.Key}
        )

class RPCArgs(BaseModel):
    ModuleRPC = ForeignKeyField(ModuleRPC, on_delete='CASCADE', related_name='args')
    name = CharField()
    RPCargtype = CharField()
    Optional = BooleanField(default=False)
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'ModuleRPC':self.ModuleRPC,'name':self.name,'RPCargtype':self.RPCargtype,'Optional':self.Optional}
        )

class Sensors(BaseModel):
    Module = ForeignKeyField(Module, on_delete='CASCADE')
    ident = CharField()
    Active = BooleanField(default=True)
    Instant = BooleanField(default=False)
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Module':self.Module,'ident':self.ident,'Active':self.Active,'Instant':self.Instant}
        )
class SensorValues(BaseModel):
    Sensor = ForeignKeyField(Sensors, on_delete='CASCADE')
    Value = CharField()
    Timestamp = DateTimeField(default=datetime.datetime.now)
    descr = None
    def jsjson(self):
        superdict = super(Module,self).jsjson()
        superdict.pop('descr')
        return dict(superdict,
        **{'Sensor':self.Sensor,'Value':self.Value,'Timestamp':self.Timestamp}
        )
    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Sensor', 'Timestamp'), True)
            )


class SensorArgs(BaseModel):
    Sensor = ForeignKeyField(Sensors, on_delete='CASCADE')
    RPCArg = ForeignKeyField(RPCArgs)
    Value = CharField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Sensor':self.Sensor,'RPCArg':self.RPCArg,'Value':self.Value}
        )

class Macros(BaseModel):
    Name = CharField()
    Value = CharField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Name':self.Name,'Value':self.Value}
        )

class Match(BaseModel):
    descr = None
    Matchstring = CharField()
    Pickled = BlobField(null=True)
    def jsjson(self):
        superdict = super(Module,self).jsjson()
        superdict.pop('descr')
        return dict(superdict,
        **{'Matchstring':self.Matchstring}
        )

class Triggers(BaseModel):
    Name = CharField()
    Match = ForeignKeyField(Match)
    Record = BooleanField()
    Lastvalue = CharField(null=True, default="null")
    
    def last(self, num=1):
        if self.Lastvalue:
            return self.Lastvalue
        else:
            return '0'
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Name':self.Name,'Match':self.Match,'Record':self.Record,'Lastvalue':self.Lastvalue}
        )

class TriggerValues(BaseModel):
    Trigger = ForeignKeyField(Triggers, on_delete='CASCADE')
    Value = CharField()
    Timestamp = DateTimeField(default=datetime.datetime.now)
    descr = None

    class meta:
        order_by = ('-Timestamp',)
        indexes = (
            (('Trigger', 'Timestamp'), True)
            )
    def jsjson(self):
        superdict = super(Module,self).jsjson()
        superdict.pop('descr')
        return dict(superdict,
        **{'Trigger':self.Trigger,'Value':self.Value,'Timestamp':self.Timestamp}
        )

class SensorFunctions(BaseModel):
    Sensor = ForeignKeyField(Sensors)
    Match = ForeignKeyField(Match)
    Function = CharField()
    Args = CharField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Sensor':self.Sensor,'Match':self.Match,'Function':self.Function,'Args':self.Args}
        )

class TriggerFunctions(BaseModel):
    Trigger = ForeignKeyField(Triggers)
    Match = ForeignKeyField(Match)
    Function = CharField()
    Args = CharField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Trigger':self.Trigger,'Match':self.Match,'Function':self.Function,'Args':self.Args}
        )

class Actions(BaseModel):
    Module = ForeignKeyField(Module)
    ident = CharField()
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Module':self.Module,'ident':self.ident}
        )

class ActionArgs(BaseModel):
    Action = ForeignKeyField(Actions)
    RPCArg = ForeignKeyField(RPCArgs)
    Value = ForeignKeyField(Match)
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Action':self.Action,'RPCArg':self.RPCArg,'Value':self.Value}
        )

class ActionsForTrigger(BaseModel):
    #mapping of triggers and actions
    Action = ForeignKeyField(Actions)
    Trigger = ForeignKeyField(Triggers)
    Match = ForeignKeyField(Match)
    def jsjson(self):
        return dict(super(Module,self).jsjson(),
        **{'Action':self.Action,'Trigger':self.Trigger,'Match':self.Match}
        )

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
                  RPCTypes,
                  ModuleRPC,
                  RPCArgs,
                  Match,
                  Sensors,
                  SensorValues,
                  SensorArgs,
                  Actions,
                  Triggers,
                  TriggerValues,
                  SensorFunctions,
                  TriggerFunctions,
                  ActionsForTrigger,
                  ActionArgs]

        for table in tables:
            try:
                table.create_table()
                print("created table:", table)
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
            if args:
                argdict = [{'name':name,
                            'RPCargtype': rpctype,
                            'Optional':opt,
                            'descr': descr,
                            'ModuleRPC': newrpc}for name, rpctype, opt, decr in args]
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

    def listModules(self):
        #returns a list of modules
        return [module for module in self.getModules()]

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
        op = {
        'last': triggerops.last,
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
