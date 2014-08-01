SYSNAME = 'domos'
EXCHANGE = 'domos'
exchange = dict(name=EXCHANGE, type='topic', durable=False, auto_delete=True, internal=False)
AMQP_URI = 'amqp://'

CORE = "domoscore"

LOGNAME = 'log'
LOG_DEBUG_TOPIC = 'log.DEBUG'
LOG_INFO_TOPIC = 'log.info'
LOG_WARN_TOPIC = 'log.warn'
LOG_ERROR_TOPIC = 'log.error'
LOG_CRIT_TOPIC = 'log.critical'

KEY_DEF = 'key'
IDENT_DEF = 'ident'
TYPE_DEF = 'type'
VALUE_DEF = 'value'

TYPE_RANGE = 'toggle'
TYPE_SINGLE = 'single'

from configparser import ConfigParser
class domosSettings:
    dbconfigpath = "db_config.cfg"
    dbname = "domos"
    default = dict(user="user",host="localhost",password="password")
    dbconfig = {}
    @staticmethod
    def getDBConfig():
        if len(domosSettings.dbconfig)==0:
            dbconfig = ConfigParser(domosSettings.default)
            dbconfig.read(domosSettings.dbconfigpath)
            if not dbconfig.has_section(domosSettings.dbname):
                dbconfig.add_section(domosSettings.dbname)
            with open(domosSettings.dbconfigpath,'w') as file:
                dbconfig.write(file)
            domosSettings.dbconfig = dict(dbconfig.items(domosSettings.dbname))      
        return domosSettings.dbconfig
