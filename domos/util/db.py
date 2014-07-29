from peewee import *
import datetime

#db = SqliteDatabase('test.db', threadlocals = True)
db = MySQLDatabase('domos', user='domos',
                   host='sql.bergzand.net',
                   password='ZwFGsHTaqRuac4sj')

rpctypes = ['list', 'get', 'del', 'add']


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


class Actions(BaseModel):
    Module = ForeignKeyField(Module)
    ident = CharField()


class ActionArgs(BaseModel):
    Sensor = ForeignKeyField(Sensors)
    Arg = CharField()
    Optional = BooleanField



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