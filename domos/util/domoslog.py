import logging as logging
import json
from threading import Thread
import domos.util.domossettings as ds
from dashi import DashiConnection
import socket
import sys

LOGKEY = 'log.#'

class rpclogger(Thread):
    
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    
    def __init__(self):
        self.done = False
        Thread.__init__(self)
        self.name = ds.LOGNAME
        self.dashi = DashiConnection(self.name, ds.AMQP_URI, ds.EXCHANGE, sysname = ds.SYSNAME)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.channels = []
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        for ch in self.channels:
            self.logger.addHandler(ch)
        #empty 
        self.logdict = {}
        self.channels.append(ch)
        self.logger = logging.getLogger('logger')
        self.logger.setLevel(logging.DEBUG)
        #register dashi handler functions
        self.dashi.handle(self.log_debug, "log_debug")
        self.dashi.handle(self.log_info, "log_info")
        self.dashi.handle(self.log_warning, "log_warn")
        self.dashi.handle(self.log_error, "log_error")
        self.dashi.handle(self.log_critical, "log_crit")
        

    def run(self):
        self.logger.log(logging.INFO, "Starting logger")
        while not self.done:
            try:
                self.dashi.consume(timeout=2)
            except socket.timeout as ex:
                pass
            
    def log_message(self, lvl, msg, handle):
        try:
            if handle:
                if handle not in self.logdict:
                    logger = logging.getLogger(handle)
                    for ch in self.channels:
                        logger.addHandler(ch)
                    logger.setLevel(logging.DEBUG)
                    self.logdict[handle] = logger
                self.logdict[handle].log(lvl, msg)
            else:
                self.logger.log(lvl, msg)
        except:
            self.logger.log(logging.DEBUG,'could not identify message' )
            self.logger.log(logging.DEBUG, msg.body )
            
    def log_debug(self, msg, handle):
        self.log_message(domoslog.DEBUG, msg, handle)

    def log_info(self, msg, handle):
        self.log_message(domoslog.INFO, msg, handle)

    def log_warning(self, msg, handle):
        self.log_message(domoslog.WARNING, msg, handle)

    def log_error(self, msg, handle):
        self.log_message(domoslog.ERROR, msg, handle)

    def log_critical(self, msg, handle):
        self.log_message(domoslog.CRITICAL, msg, handle)

    def end(self):
        self.done = True

class domoslogger:

    def __init__(self, connpool=None):
        self.shutdown = False
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.ch = logging.StreamHandler()
        self.ch.setFormatter(formatter)
        self.ch.setLevel(logging.DEBUG)
        self.loghandler = logging.getLogger('domos')
        self.loghandler.setLevel(logging.DEBUG)
        self.loghandler.addHandler(self.ch)
        self.startmq(connpool)

    def _onRequest(self,body, msg):
        logmsg = body
        try:
            self.loghandler.log(logmsg['level'],logmsg['message'])
        except:
            self.loghandler.log(logging.DEBUG,'could not identify message' )
            self.loghandler.log(logging.DEBUG, msg.body )

            
    def startmq(self, connpool):
        exchange = Exchange(**ds.exchange)
        control_queue = Queue(exchange=exchange, routing_key=LOGKEY, exclusive=True)
        if connpool:
            self.conn = connpool.acquire(block=True)
        else:
            self.conn = Connection(ds.AMQPURI)
        with self.conn as conn:
            with conn.Consumer(control_queue, callbacks=[self._onRequest]) as consumer:
                while not self.shutdown:
                    conn.drain_events()


class domoslog:
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

    def __init__(self, queue=None):
        self.queue = queue
        if self.queue:
            self.log_message = self.queue_message
        else:
            self.log_message = self.return_message


    def queue_message(self, level, msg):
        self.queue.put(('log',{'level': level, 'message': msg}))
        return True 

    def return_message(self, level, msg):
        return {'level': level, 'message': msg}

    def log_debug(self, msg):
        return self.log_message(domoslog.DEBUG, msg)

    def log_info(self, msg):
        return self.log_message(domoslog.INFO, msg)

    def log_warning(self, msg):
        return self.log_message(domoslog.WARNING, msg)

    def log_error(self, msg):
        return self.log_message(domoslog.ERROR, msg)

    def log_critical(self, msg):
        return self.log_message(domoslog.CRITICAL, msg)
