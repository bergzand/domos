
from peewee import *
import datetime



#db = SqliteDatabase('test.db', threadlocals = True)
db = Proxy()

rpctypes = ['list', 'get', 'del', 'add', 'set']


class BaseModel(Model):
    descr = TextField(null=True)

    class Meta:
        database = db

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
    Module = ForeignKeyField(Module)
    RPCType = ForeignKeyField(RPCTypes)
    Key = CharField()


class RPCArgs(BaseModel):
    ModuleRPC = ForeignKeyField(ModuleRPC)
    name = CharField()
    RPCargtype = CharField()
    Optional = BooleanField(default=False)



class Sensors(BaseModel):
    Module = ForeignKeyField(Module)
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


def init_dbconn(conf):
    try:
        driver = conf.pop('driver')
        database = conf.pop('database')
    except:
        pass
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
        db.initialize(databaseconn)
    else:
        raise ImproperlyConfigured("Cannot not initialize database connection")

def create_tables():
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


def init_tables():
    for type in rpctypes:
        try:
            RPCTypes.get(RPCTypes.rpctype == type)
        except RPCTypes.DoesNotExist:
            newtype = RPCTypes()
            newtype.rpctype = type
            newtype.save()