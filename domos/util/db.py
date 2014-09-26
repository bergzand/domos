
from peewee import *
import datetime
import math
import statistics

dbconn = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']

#Peewee database models


class BaseModel(Model):
    """Base model of the database, provides default settings
    """
    translations = [('id', 'id')]

    class Meta:
        database = dbconn

    @classmethod
    def get_by_id(cls, num):
        """Returns the object with the specified database ID
        :param num: The ID of the object
        """
        return cls.get(cls.id == num)

    def to_dict(self, **kwargs):
        print(self.__class__)
        if issubclass(self.__class__, BaseModel):
            ts = super(self.__class__, self).translations
        else:
            ts = []
        t = ts+self.translations
        print(t)
        d = {name: getattr(self, variable) for variable, name in t}
        if 'deep' in kwargs:
            for parameter in kwargs['deep']:
                dd = kwargs['deep']
                if(hasattr(self,parameter)):
                    dd.remove(parameter)
                    if type(getattr(self,parameter))is list:
                        l=[i.to_dict(deep=dd) for i in getattr(self,parameter)]
                    else:
                        l =  getattr(self,parameter).to_dict()
                    d.update({parameter:l})
        return d
    
    def from_dict(self,dict,**kwargs):
        print(self.__class__)
        if issubclass(self.__class__, BaseModel):
            ts = super(self.__class__, self).translations
        else:
            ts = []
        t = ts+self.translations
        print(t)
        for variable, name in t:
            setattr(self,variable,dict[name]) 
        if 'deep' in kwargs:
            for parameter in kwargs['deep']:
                dd = kwargs['deep']
                if(hasattr(self,parameter)):
                    print('deeping '+parameter)
                    dd.remove(parameter)
                    if type(getattr(self,parameter))is list:
                        print('islist')
                        setattr(self,parameter,[])
                        for i in dict[parameter] :
                             getattr(self,parameter).append(Sensor().from_dict(i,deep=dd))
                    else:
                        print('isvariable')
                        getattr(self,parameter).from_dict(dict[parameter],deep=dd)
        return self

class Module(BaseModel):
    """Model of a module

    * Name: Name of the module
    * queue: message queue the module is listening on
    * Active: whether the module is active or not
    """
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
        """Add a module to the database

        :param name: The name of the module
        :param queue: The message queue to reach the module on
        :param active: Set the module as active or inactive
        """
        return cls.create(name=name, queue=queue, Active=active)

    @classmethod
    def list(cls):
        """returns a list of modules
        """
        return [module for module in Module.select()]

    @classmethod
    def get_by_name(cls, name):
        """Returns the :class:`Module` object with the specified name. throws an peewee.DoesNotExist exception if the module does not exist.

        :param name: The name of the :class:`Module`
        :rtype: A :class:`Module` object.
        """
        return cls.get(Module.name == name)


class RPCType(BaseModel):
    """Available types of RPC calls
    see also the rpctypes variable
    """
    translations = [('rpctype', 'rpctype'),
                    ('desc', 'des')]
    rpctype = CharField()
    desc = TextField(null=True)


class ModuleRPC(BaseModel):
    """RPC a :class:`Module` supports.

    * Module: foreign key to a :class:`Module`
    * RPCType: foreign, the type of :class:`RPCType`
    * key: Key to distiguish multiple rpc's of the same type
    """
    translations = [('key', 'key'),
                    ('desc', 'des')]
    module = ForeignKeyField(Module, related_name='rpcs', on_delete='CASCADE')
    rpctype = ForeignKeyField(RPCType)
    key = CharField()
    desc = TextField(null=True)

    @classmethod
    def add(cls, module, key, rpctype, args, desc=None):
        """ Adds an rpc command to the database

            :param module: module object
            :param key: name of the rpc
            :param rpctype: string of the type of the rpc
            :param args: list of tuples, (name, type, optional, desc)
            :param desc: description of the rpc request
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
        """ Retrieve RPC's associated with a module
        
        :param module: :class`Module` to retrieve remote procedures for
        :param type: type of rpc to return
        :rtype: Iterator with remote procedures
        """
        rtrn = None
        if type:
            rtrn = cls.select(cls).join(RPCType).where((cls.module == module) & (RPCType.rpctype == type))
        else:
            rtrn = cls.select(cls).join(RPCType).where((cls.module == module) & (RPCType.rpctype == type))
        return rtrn


class RPCArg(BaseModel):
    """Arguments for a RPC

    * modulerpc: foreign key to a :class:`ModuleRPC`
    * name: Name of the argument, used in the dictionary send to the module
    * rpcargtype: Type of argument, eg: string, integer
    * optional: Whether the argument is optional
    * desc: description of this argument
    """
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
        """Query the database for remote procedure arguments of the specified module and of a specified type
           
        """
        return RPCArg.select().join(ModuleRPC).join(RPCType).where((ModuleRPC.Module == module) &
                                                                   (RPCType.rpctype == 'add'))


class Sensor(BaseModel):
    """Sensor to measure
    
    * modulerpc: :class:`ModuleRPC` associated with the sensor
    * ident: identifier of the sensor
    * active: Whether the sensor is active or disabled
    * instant: is the sensor of the type Instant
    """
    translations = [('name', 'name'),
                    ('active', 'active'),
                    ('instant', 'instant'),
                    ('desc', 'des')]
    modulerpc = ForeignKeyField(ModuleRPC, related_name='sensors', on_delete='CASCADE')
    name = CharField()
    active = BooleanField(default=True)
    instant = BooleanField(default=False)
    desc = TextField(null=True)

    @classmethod
    def add(cls, module, name, argdata):
        """Add a sensor to the database

        :param module: The :class:`Module` to add the sensor to
        :param name: An human readable identifier, ie: temp_outside
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
        """Return all sensors of the specified :class:`module`
        
        :param module: The :class:`Module` object or ID to
        :rtype: An iterator with :class:`Sensor` classes
        """
        return Sensor.select(Sensor, ModuleRPC).join(ModuleRPC).where(ModuleRPC.module == module)

    @classmethod
    def get_by_name(cls, name):
        """Query the database for a :class:`Sensor` with the specified name.
        .. note::

            This returns only the first :class:`Sensor` with this name

        :param name: The name to query for
        :rtype: A :class:`Sensor` class
        """
        return Sensor.get(Sensor.name == name)

    def add_value(self, value):
        """add a :class:`SensorValue` to the database for this :class:`Sensor`
        
        :param value: The value to add to the database
        """
        value = SensorValue.create(sensor=self, value=str(value))

    def lastrecords(self, num):
        """Returns a list of the last values of this sensor
        
        :param num: Amount of :class:`SensorValue` to return
        :rtype: an iterator over the values
        """
        if self.instant:
            return []
        else:
            return SensorValue.select().where(SensorValue.sensor == self).order_by(SensorValue.timestamp.desc()).limit(num).naive()


class SensorValue(BaseModel):
    """Values of a sensor, represents a measurement value
    of the sensor at a certain point in time

    * sensor: associated :class:`Sensor`
    * value: measurement value
    * timestamp: Point in time of the value
    """
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

    @classmethod
    def insert_many(cls, rows):
        return super().insert_many(rows)


class SensorArg(BaseModel):
    """Argument of a sensor, combination of a :class:`RPCArg` and a :class:`Sensor`

    * sensor: The :class:`Sensor` for which the argument is
    * rpcarg: The argument, of type :class:`RPCArg`
    * value: The value of this argument
    """
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
        """This function creates a dict from the arguments of a :class:`sensor`
        .. note::

            A argument get separated by dots. ie: start.year becomes: {'start':{'year': value}}

        :param sensor: The sensor to retrieve the argument dictionary for
        :rtype: A dict with argument keys and values
        """
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
    """Macro table, nothing more that key-value pairs for use in :class:`Expression` as constants
    """
    translations = [('name', 'name'), ('value', 'value')]
    name = CharField()
    value = CharField()


class Expression(BaseModel):
    """Table that contains expressions used by other tables

    * expression: String containing the expression
    * pickled: binary blob containing the pickled AST of the expression
    """
    translations = [('expression', 'expression')]
    expression = CharField()
    pickled = BlobField(null=True)

    def get_used_sensors(self):
        return VarSensor.select(VarSensor, Sensor).join(Sensor).where(VarSensor.expression == self)

    def get_used_triggers(self):
        return VarTrigger.select(VarTrigger, Trigger).join(Trigger).where(VarTrigger.expression == self)


class Trigger(BaseModel):
    """Table with triggers

    * name: Name of the trigger
    * expression: foreign, class:`Expression` on which the trigger activates
    * record: Whether to log this trigger to the triggervalues table
    * lastvalue: Last calculated value of this trigger
    """
    translations = [('name', 'name'),
                    ('record', 'record'),
                    ('lastvalue', 'lastvalue')]
    name = CharField()
    expression = ForeignKeyField(Expression)
    record = BooleanField()
    lastvalue = CharField(null=True, default="null")

    def get_affected_triggers(self):
        """Returns all triggers that have this trigger in their :class:`Expression`
        .. note::

            This function will never return itself as trigger result
        
        :rtype: an iterator with :class:`Trigger` classes
        """
        return Trigger.select(Trigger, Expression).join(Expression)\
            .join(VarTrigger)\
            .where(
                (VarTrigger.source == self) &
                (Trigger != self)
                )

    @classmethod
    def get_affected_by_trigger(cls, trigger):
        """Returns all :class:`Trigger` affected by the specified trigger.
        
        .. note::
            
            This function will never return the specified trigger in its results
        
        :param trigger: The :class:`Trigger` to query for
        :rtype: an iterator with affected :class:`Trigger`
        """
        return Trigger.select(Trigger, Expression).join(Expression)\
            .join(VarTrigger)\
            .where(
                (VarTrigger.source == trigger) &
                (Trigger.id != trigger)
                )

    @classmethod
    def get_affected_by_sensor(cls, sensor):
        """Returns all :class:`Trigger` affected by the specified :class:`Sensor`.
        
        :param sensor: The sensor to query for
        :rtype: an iterator of affected :class:`Trigger` classes
        """
        return Trigger.select(Trigger, Expression).join(Expression).join(VarSensor, JOIN_INNER).where(VarSensor.source == sensor)

    def add_value(self, value):
        """Add a value to the trigger

        :param value: The value to add to this :class:`Trigger`
        """
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
    """values of triggers
    
    * trigger: :class:`Trigger` to which this value belongs
    * value: The actual value
    * timestamp: Time at which this value was calculated
    """
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
    """Mapping of :class:`Sensor` used by :class:`Expression`
    
    * source: which :class:`Sensor` to which...
    * expression: :class:`Expression` using this sensor
    * function: Which function to use on the values of this sensor
    * args: Tuple of arguments for the sensor function
    """
    translations = [('function', 'function'),
                    ('args', 'args')]
    source = ForeignKeyField(Sensor, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class VarTrigger(BaseModel):
    """Mapping of :class:`Trigger` used by :class:`Expression`
    
    * source: which :class:`Trigger` to which...
    * expression: :class:`Expression` using this trigger
    * function: Which function to use on the values of this sensor
    * args: Tuple of arguments for the sensor function
    """
    translations = [('function', 'function'),
                    ('args', 'args')]
    source = ForeignKeyField(Trigger, related_name='functions')
    expression = ForeignKeyField(Expression)
    function = CharField()
    args = CharField()


class Action(BaseModel):
    """Actions to send to a module
    modulerpc: The :class:`ModuleRPC` to send the action to
    ident: Identifier for this action
    """
    translations = [('name', 'name')]
    modulerpc = ForeignKeyField(ModuleRPC, related_name='actions')
    name = CharField()


class ActionArg(BaseModel):
    """Arguments to send with an action RPC
    action: :class:`Action` to which the arguments belong
    rpcargs: The argument
    value: Value of this argument
    """
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
        """This function creates a dict from the arguments of a :class:`Action`
        .. note::

            A argument get separated by dots. ie: start.year becomes: {'start':{'year': value}}

        :param sensor: The :class:`Action` to retrieve the argument dictionary for
        :rtype: A dict with argument keys and values
        """
        actionargs = cls.select().where(cls.action == action)
        kwargs = {}
        for act in actionargs:
            value = calculator.resolve(act.value)
            rpcarg = act.rpcarg
            key, value = cls._to_dict(kwargs, rpcarg.name, value)
            kwargs[key] = value
        return kwargs

class TriggerAction(BaseModel):
    """:class:`Trigger` to :class:`Action` mapping

    * action: The :class:`Action` to trigger
    * trigger: which :class:`Trigger` triggers this action
    * expression: Expression, the action is only executed 

    if triggered and if the expression resolves to a non-False value
    """
    
    action = ForeignKeyField(Action, related_name='triggers')
    trigger = ForeignKeyField(Trigger, related_name='actions')
    expression = ForeignKeyField(Expression)
    
    @classmethod
    def get_by_trigger(cls, trigger):
        return cls.select(cls, Action).join(Action).where(cls.trigger == trigger)


class dbhandler:
    """Database connection and settings handler class.
       Only specify a configuration or a database

       :param conf: Dictionary to configure a database connection with. It
                   should contain at least a database driver to use and
                   the name of the database. Other parameters are passed
                   to the database driver
       :param database: Database object to use. It should be a peewee-usable database object
    """

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
        """Try to create a set of empty tables, silently fails if the table already exists
        """
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
        """Initialize any preconfigured content that is needed for initial
           start up. Currently only the RPCTypes table is filled and needed;
           All :class:`Module` are configured as inactive
        """
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
