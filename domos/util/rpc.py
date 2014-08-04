from dashi import DashiConnection
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
import socket


class rpc:
    def __init__(self, name, loghandle=None):
        self.name = name
        if loghandle:
            self.loghandle = loghandle
        else:
            self.loghandle = name
        dashiconfig = domosSettings.getDashiConfig()
        self.dashi = DashiConnection(self.name, dashiconfig["amqp_uri"], dashiconfig['exchange'], sysname = dashiconfig['sysname'])

    def handle(self, func, handle):
        self.dashi.handle(func, handle)

    def fire(self, target, func, **kwargs):
        self.dashi.fire(target, func, **kwargs)

    def call(self, target, func, **kwargs):
        return self.dashi.call(target, func, **kwargs)

    def logmsg(self, lvl, msg):
        call = 'log_{}'.format(lvl)
        self.fire('log', call, msg=msg, handle=self.loghandle)

    def log_debug(self, msg):
        self.fire('log', 'log_debug', msg=msg, handle=self.loghandle)

    def log_info(self, msg):
        self.fire('log', 'log_info', msg=msg, handle=self.loghandle)

    def log_warn(self, msg):
        self.fire('log', 'log_warn', msg=msg, handle=self.loghandle)

    def log_error(self, msg):
        self.fire('log', 'log_error', msg=msg, handle=self.loghandle)

    def log_crit(self, msg):
        self.fire('log', 'log_crit', msg=msg, handle=self.loghandle)

    def listen(self, timeout=2):
        try:
            self.dashi.consume(timeout=timeout)
        except socket.timeout as ex:
            pass
