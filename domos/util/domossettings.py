from configparser import ConfigParser
import logging
import shlex


class domosSettings:
    # sections
    dbsection = "database"
    coresection = "core"
    loggingsection = "logging"
    dashisection = "exchange"
    websection = "webserver"

    dbdefault = dict(user="user", host="localhost", password="password", driver="mysql", database="domos")
    defaultconfig = 'defaults.cfg'
    configpath = "domos.cfg"

    dashidefault = dict(sysname="domos", exchange="domos", amqp_uri="amqp://")
    config = None

    @staticmethod
    def setConfigFile(file):
        domosSettings.configpath = file

    @staticmethod
    def reloadConfig():
        pass

    @staticmethod
    def _readConfig():
        if not domosSettings.config:
            config = ConfigParser()
            config.read_file(open(domosSettings.defaultconfig))
            try:
                config.read(domosSettings.configpath)
            except TypeError:
                #print('No config supplied, using defaults')
                pass
            domosSettings.config = config
        return domosSettings.config

    @staticmethod
    def getDBConfig():
        configmapping = [('driver', 'driver', 'sqlite'),
                         ('database', 'database', 'domos.db'),
                         ('host', 'host', None),
                         ('user', 'username', None),
                         ('password', 'password', None)]
        config = domosSettings._readConfig()
        cfg = {}
        for dictmap, configmap, default in configmapping:
            cfg[dictmap] = config.get(domosSettings.dbsection, configmap, fallback=default)
        return cfg

    @staticmethod
    def getSecretKey():
        config = domosSettings._readConfig()
        cfg = {}
        cfg['secret_key'] = config.get(domosSettings.websection, 'secret_key', fallback='shhh')
        return cfg

    @staticmethod
    def getExchangeConfig():
        config = domosSettings._readConfig()
        return dict(config.items(domosSettings.dashisection))

    @staticmethod
    def get_core_config():
        config = domosSettings._readConfig()
        cfg = {}
        cfg['module_dir'] = shlex.split(config.get(domosSettings.coresection, 'module_dir', fallback = None))
        return cfg

    @staticmethod
    def getLoggingConfig():
        config = domosSettings._readConfig()
        cfg = {}
        files = config.get(domosSettings.loggingsection, 'logfiles', fallback='stdout')
        logfiles = [tuple(logfile.split(':', 1)) for logfile in shlex.split(files)]
        cfg['logfiles'] = logfiles
        cfg['stdout'] = config.getboolean(domosSettings.loggingsection, 'stdoutlog', fallback=False)
        cfg['defaultlevel'] = config.get(domosSettings.loggingsection, 'defaultlevel', fallback='warning')
        levels = config.get(domosSettings.loggingsection, 'loglevels', fallback='warn')
        cfg['loglevels'] = [tuple(loglevel.split(':', 1)) for loglevel in shlex.split(levels)]
        return cfg

    # noinspection PyNoneFunctionAssignment
    @staticmethod
    def getLoggingLevel(logger):
        loglevels = {'debug': logging.DEBUG,
                     'info': logging.INFO,
                     'warning': logging.WARNING,
                     'error': logging.ERROR,
                     'critical': logging.CRITICAL}
        config = domosSettings._readConfig()
        levels = config.get(domosSettings.loggingsection, 'loglevels', fallback='')
        loglevel = logging.WARNING
        for logsetting in shlex.split(levels):
            part, level = logsetting.split(':', 1)
            if part == logger:
                loglevel = loglevels[level]
                break
        else:
            loglevel = loglevels[config.get(domosSettings.loggingsection, 'defaultlevel', fallback='warning')]
        return loglevel
