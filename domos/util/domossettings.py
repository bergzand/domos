from configparser import ConfigParser


class domosSettings:
    dbconfigpath = "db_config.cfg"
    dbname = "domos"
    dbdefault = dict(user="user", host="localhost", password="password")

    configpath = "dashiconfig.cfg"
    dashisection = "dashi"
    dashidefault = dict(sysname="domos", exchange="domos", amqp_uri="amqp://")
    dbconfig = None
    dashiconfig = None

    @staticmethod
    def getDBConfig():
        if not(domosSettings.dbconfig):
            dbconfig = ConfigParser(domosSettings.dbdefault)
            dbconfig.read(domosSettings.dbconfigpath)
            if not dbconfig.has_section(domosSettings.dbname):
                dbconfig.add_section(domosSettings.dbname)
            with open(domosSettings.dbconfigpath, 'w') as file:
                dbconfig.write(file)
            domosSettings.dbconfig = dict(dbconfig.items(domosSettings.dbname))
        return domosSettings.dbconfig

    @staticmethod
    def getDashiConfig():
        if not(domosSettings.dashiconfig):
            config = ConfigParser(domosSettings.dashidefault)
            config.read(domosSettings.configpath)
            if not config.has_section(domosSettings.dashisection):
                config.add_section(domosSettings.dashisection)
            with open(domosSettings.configpath, 'w') as file:
                config.write(file)
            domosSettings.dashiconfig = dict(config.items(domosSettings.dashisection))
        return domosSettings.dashiconfig
