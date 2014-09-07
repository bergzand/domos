import logging as logging
import json
from threading import Thread
import domos.util.domossettings as ds
from domos.util.domossettings import domosSettings
from dashi import DashiConnection
import socket
import sys
from pprint import pprint

class rpclogger(Thread):
    
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    
    def __init__(self):
        self.done = False
        Thread.__init__(self)
        self.name = "log"
        dashiconfig = domosSettings.getDashiConfig()
        self.dashi = DashiConnection(self.name, dashiconfig["amqp_uri"], dashiconfig['exchange'], sysname = dashiconfig['sysname'])
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.ch = logging.StreamHandler()
        self.ch.setFormatter(formatter)
        self.ch.setLevel(logging.DEBUG)
        #register dashi handler functions
        self.dashi.handle(self.logrecord, "logrecord")

    def run(self):
        while not self.done:
            try:
                self.dashi.consume(timeout=2)
            except socket.timeout as ex:
                pass

    def logrecord(self, record):
        logrecord = logging.makeLogRecord(record)
        self.ch.handle(logrecord)

    def end(self):
        self.done = True


class rpchandler(logging.Handler):
    def __init__(self, rpc, queue='log'):
        logging.Handler.__init__(self)
        self.dashi = rpc
        self.queue = queue

    def emit(self, record):
        d = dict(record.__dict__)
        d['msg'] = record.getMessage()
        d['args'] = None
        d['exc_info'] = None

        self.dashi.fire(self.queue, 'logrecord', record=d)