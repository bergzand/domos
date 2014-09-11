
from peewee import *
import datetime
import math
try:
    import statistics
except:
    statistics = None

dbconn = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']

"""
Peewee database models
"""


class BaseModel(Model):
    """Base model of the database, provides default settings
    """
    descr = TextField(null=True)

    class Meta:
        database = dbconn

    @classmethod
    def get_by_id(cls, num):
        return cls.get(cls.id == num)


class Module(BaseModel):
    """Model of a module

    * Name: Name of the module
    * queue: message queue the module is listening on
    * Active: whether the module is active or not

    """
    name = CharField()
    queue = CharField()
    Active = BooleanField()


class RPCTypes(BaseModel):
    """Available types of RPC calls
    see also the rpctypes variable
    """
    rpctype = CharField()


class ModuleRPC(BaseModel):
    """RPC a module supports. 

    * Module: foreign key to a :class:`Module`
    * RPCType: foreign, the type of :class:`RPCType`
    * key: Key to distiguish multiple rpc's of the same type
    """
    Module = ForeignKeyField(Module, on_delete='CASCADE')
    RPCType = ForeignKeyField(RPCTypes)
    Key = CharField()


class RPCArgs(BaseModel):
    """Arguments for a RPC

    * ModuleRPC: foreign key to a :class:`ModuleRPC`
    * name: Name of the argument, used in the dictionary send to the module
    * RPCargtype: Type of argument, eg: string, integer
    * Optional: Whether the argument is optional
    """
    ModuleRPC = ForeignKeyField(ModuleRPC, on_delete='CASCADE', related_name='args')
    name = CharField()
    RPCargtype = CharField()
    Optional = BooleanField(default=False)


class Sensors(BaseModel):
    """Sensor
    
    * Module: :class:`Module` associated with the sensor
    * Ident: identifier of the sensor
    * Active: Whether the sensor is active or disabled
    * Instant: is the sensor of the type Instant
    """
    Module = ForeignKeyField(Module, on_delete='CASCADE')
    ident = CharField()
    Active = BooleanField(default=True)
    Instant = BooleanField(default=False)


class SensorValues(BaseModel):
    """Values of a sensor, represents a measurement value 
    of the sensor at a certain point in time

    * Sensor: associated :class:`Sensor`
    * Value: measurement value
    * Timestamp: Point in time of the value
    """
    Sensor = ForeignKeyField(Sensors, on_delete='CASCADE')
    Value = CharField()
    Timestamp = DateTimeField(default=datetime.datetime.now)
    descr = None

    class meta:
        order_by = ('-Timestamp',)
        # Combined index on sensor and timestamp, useful
        # for ordering on time per sensor
        indexes = (
            (('Sensor', 'Timestamp'), True)
            )


class SensorArgs(BaseModel):
    """Argument of a sensor, combination of a :class:`RPCArg` and a :class:`Sensor`
    
    * Sensor: The :class:`Sensor` for which the argument is
    * RPCArg: The argument, of type :class:`RPCArgs`
    * Value: The value of this argument
    """
    Sensor = ForeignKeyField(Sensors, on_delete='CASCADE')
    RPCArg = ForeignKeyField(RPCArgs)
    Value = CharField()


class Macros(BaseModel):
    """Macro table, nothing more that key-value pairs
    """
    Name = CharField()
    Value = CharField()


class Match(BaseModel):
    """Table that contains expressions used by other tables
    
    * Matchsting: String containing the expression
    * Pickled: binary blob containing the pickled AST of the expression
    """
    descr = None
    Matchstring = CharField()
    Pickled = BlobField(null=True)


class Triggers(BaseModel):
    """Table with triggers

    * Name: Name of the trigger
    * Match: foreign, class:`Match` on which the trigger activates
    * Record: Whether to log this trigger to the triggervalues table
    * Lastvalue: Last calculated value of this trigger
    """
    Name = CharField()
    Match = ForeignKeyField(Match)
    Record = BooleanField()
    Lastvalue = CharField(null=True, default="null")


class TriggerValues(BaseModel):
    """Values of triggers
    
    * Trigger: :class:`Triggers` to which this value belongs
    * Value: The actual value
    * Timestamp: Time at which this value was calculated
    """
    Trigger = ForeignKeyField(Triggers, on_delete='CASCADE')
    Value = CharField()
    Timestamp = DateTimeField(default=datetime.datetime.now)
    descr = None

    class meta:
        order_by = ('-Timestamp',)
        # Combined index on sensor and timestamp, useful
        # for ordering on time per sensor
        indexes = (
            (('Trigger', 'Timestamp'), True)
            )


class SensorFunctions(BaseModel):
    """Mapping of Sensors used by expressions
    
    * Sensor: which :class:`Sensors` to which...
    * Match: :class:`Match` using this sensor
    * Function: Which function to use on the values of this sensor
    * Args: Tuple of arguments for the sensor function
    """
    Sensor = ForeignKeyField(Sensors)
    Match = ForeignKeyField(Match)
    Function = CharField()
    Args = CharField()


class TriggerFunctions(BaseModel):
    """Mapping of triggers used by expressions
    
    * Trigger: which :class:`Triggers` to which...
    * Match: :class:`Match` using this sensor
    * Function: Which function to use on the values of this sensor
    * Args: Tuple of arguments for the sensor function
    """
    Trigger = ForeignKeyField(Triggers)
    Match = ForeignKeyField(Match)
    Function = CharField()
    Args = CharField()


class Actions(BaseModel):
    """Actions to send to a module
    Module: The :class:`Module` to send the action to
    Ident: Identifier for this action
    """
    Module = ForeignKeyField(Module)
    ident = CharField()


class ActionArgs(BaseModel):
    """Arguments to send with an action RPC
    Action: Action to which the arguments belong
    RPCArgs: The argument
    Value: Value of this argument
    """
    Action = ForeignKeyField(Actions)
    RPCArg = ForeignKeyField(RPCArgs)
    Value = ForeignKeyField(Match)


class ActionsForTrigger(BaseModel):
    """Triggers to trigger an action

    * Action: The action to trigger
    * Trigger: which :class:`Triggers` triggers this action
    * Match: Expression, the action is only executed 

    if triggered and if the expression resolves to a non-False value
    """
    Action = ForeignKeyField(Actions)
    Trigger = ForeignKeyField(Triggers)
    Match = ForeignKeyField(Match)


class dbhandler:
    """Database handler class
    """

    def __init__(self, conf=None, database=None):
        """Initialize a database connection with a specified config or database object.
        Only specify a configuration or a database
        
        :param conf: Dictionary to configure a database connection with. It 
                     should contain at least a database driver to use and 
                     the name of the database. Other parameters are passed 
                     to the database driver
        :param database: Database object to use. It should be a peewee-usable database object
        """
        self.connected = False
        if database:
            databaseconn = database
            dbconn.initialize(databaseconn)
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
        """Try to create a set of empty tables, silently fails if the table already exists
        """
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
        """Initialize any preconfigured content that is needed for initial 
        start up. Currently only the RPCTypes table is filled and needed;
        All :class:`Module` are configured as inactive
        """
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
        """Open a connection to the database
        """
        conn = dbconn.connect()
        self.connected = True
        return conn

    def close(self):
        """Close the connection to the database
        """
        conn = dbconn.close()
        self.connected = False
        return conn

    def addModule(self, name, queue, active=True):
        """Add a module to the database.
        
        :param name: Name of the :class:`Module`
        :param queue: queue to use at the message broker
        :param active: Initialize the module as activated or not
        :rtype: The created :class:`Module`
        """
        return Module.create(name=name, queue=queue, Active=active)

    def addRPC(self, module, key, rpctype, args, descr=None):
        """Adds an rpc command to the database
        
        :param module: :class:`Module` object
        :param key: name of the :class:`ModuleRPC`
        :param rpctype: string of the type of the rpc
        :param args: list of tuples, (name, type, optional, descr)
        :param descr: description of the rpc request
        """

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
        """
        """
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
        """returns an iterator with Module objects

        :rtype: An one use iterator with :class:`Module` objects
        """
        return Module.select().naive().iterator()

    def listModules(self):
        """returns a list with :class:`Module` objects

        :rtype: A list with :class:`Module`
        """
        return [module for module in self.getModules()]

    def getModule(self, modulename):
        #returns Module object with modulename
        return Module.get(Module.name == modulename)

    def getModuleByID(self, id):
        return Module.get_by_id(id)

    def getRPCs(self, module, type):
        return ModuleRPC.select(ModuleRPC).join(RPCTypes).where((ModuleRPC.Module == module) & (RPCTypes.rpctype == type))
        
    def getRPCCall(self, module, type):
        """
        """
        return ModuleRPC.select().join(RPCTypes).where((ModuleRPC.Module == module) & (RPCTypes.rpctype == type)).limit(1)[0]

    def addSensor(self, module, identifier, argdata):
        """Add a sensor to the database
            argdata: list of dicts with name=value pairs
        """
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
        """
        """
        sensortriggers = Triggers.Select().Join(Functions).Where(Functions.Sensor == sensor)
        for sensortrigger in sensortriggers:
            pass
            #check this sensor on true

    def getActionsfromtrigger(self, trigger_id):
        """
        """
        actions = ActionsForTrigger.select(ActionsForTrigger, Actions).join(Actions).where(ActionsForTrigger.Trigger == trigger_id)
        return [action for action in actions]


class sensorops:
    
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
            if statistics:
                result = statistics.mean((int(i.Value) for i in selection))
                return result
            else:
                return 0
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

class triggeroperations:
    
    @staticmethod
    def last(trigger, num):
        pass

class sensorerror(Exception):                                    
    def __init__(self, sensor, message):
        self.sensor = sensor
        self.message = message
