#!/usr/bin/env python3

from threading import Thread
import domos.util.domossettings as ds
from domos.util.rpc import rpc
from domos.util.domoslog import rpchandler
from dashi import DashiConnection
import socket
import logging
from multiprocessing import Process
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from collections import namedtuple

ROUTING_KEY = 'sensor.time'
CONTROL_KEY = 'control.time'

DOMOSTIME_DICT = {
    "name": "domosTime",
    "queue": "domosTime",
    "rpc": [
        {"key": "getTimers", "type": "list"},
        {"key": "getTimer", "type": "get", "args": [

        ]
        },
        {"key": "delTimer", "type": "del", "args": [

        ]
        },
        {"key": "addTimer", "type": "add", "args": [
            {"name": "start.year", "type": "string", "optional": "True"},
            {"name": "start.month", "type": "string", "optional": "True"},
            {"name": "start.week", "type": "string", "optional": "True"},
            {"name": "start.day", "type": "string", "optional": "True"},
            {"name": "start.day_of_week", "type": "string", "optional": "True"},
            {"name": "start.hour", "type": "string", "optional": "True"},
            {"name": "start.minute", "type": "string", "optional": "True"},
            {"name": "start.second", "type": "string", "optional": "True"},
            {"name": "stop.year", "type": "string", "optional": "True"},
            {"name": "stop.month", "type": "string", "optional": "True"},
            {"name": "stop.week", "type": "string", "optional": "True"},
            {"name": "stop.day", "type": "string", "optional": "True"},
            {"name": "stop.day_of_week", "type": "string", "optional": "True"},
            {"name": "stop.hour", "type": "string", "optional": "True"},
            {"name": "stop.minute", "type": "string", "optional": "True"},
            {"name": "stop.second", "type": "string", "optional": "True"},
        ]
        },
    ],
}


class DomosTime(Process):
    rpc = namedtuple('rpc', ['func', 'call'])



    def __init__(self):

        self.domostime_rpc = {"getTimers": self.get_jobs,
                         "getTimer": self.get_job,
                         "addTimer": self.add_job,
                         "delTimer": self.del_job,
                        }


        Process.__init__(self)
        self.done = False
        self.name = 'domosTime'
        self._items = []
        self._sched = None
        self.rpc = None


    def init_scheduler(self):
        self._sched = BackgroundScheduler()

    def initrpc(self):
        self.rpc = rpc(self.name)
        self.logger = logging.getLogger('DomosTime')
        self.logger.setLevel(logging.DEBUG)
        loghandler = rpchandler(self.rpc)
        self.logger.addHandler(loghandler)

        for call, func in self.domostime_rpc.items():
            self.rpc.handle(func, call)
        self.logger.debug('Initializing scheduler')


    def add_job(self, key=None,
               name=None, jobtype='Once',
               start=None, stop=None):
        returnvalue = False
        for job in self._items:
            if job['key'] == key:
                # duplicate job found
                break
        else:
            # no duplicates found
            newjob = dict()
            if stop:
                # True job
                newjob['start'] = self._sched.add_job(self._job_true,
                                                      args=[key, name],
                                                      trigger='cron',
                                                      name=name,
                                                      **start)
                #false job
                newjob['stop'] = self._sched.add_job(self._job_false,
                                                     args=[key, name],
                                                     trigger='cron',
                                                     name=name,
                                                     **stop)
                newjob['name'] = name
                newjob['key'] = key
                newjob['type'] = jobtype
                returnvalue = True
            else:
                newjob['start'] = self._sched.add_job(self._job_once,
                                                      args=[key, name],
                                                      trigger='cron',
                                                      name=name,
                                                      **start)
                newjob['stop'] = None
                newjob['name'] = name
                newjob['key'] = key
                newjob['type'] = jobtype
                returnvalue = True
            self._items.append(newjob)
        return returnvalue

    def _job_true(self, key, name):
        self.rpc.fire("domoscore", 'sensorValue', key=key, value='1')

    def _job_false(self, key, name):
        self.rpc.fire("domoscore", 'sensorValue', key=key, value='0')

    def _job_once(self, key, name):
        self.rpc.fire("domoscore", 'sensorValue', key=key, value='1')

    def get_jobs(self):
        self.logger.debug("All jobs requested")
        jobs = []
        for job in self._items:
            one = dict()
            one['start'] = dict()
            for field in job['start'].trigger.fields:
                one['start'][field.name] = str(field)
            if job['stop']:
                one['stop'] = dict()
                for field in job['stop'].trigger.fields:
                    one['stop'][field.name] = str(field)
            else:
                one['stop'] = None
            one['name'] = job['name']
            one['type'] = job['type']
            one['key'] = job['key']
            jobs.append(one)
        return jobs

    def del_job(self, key=None):
        self.logger.debug("one job deletion requested")
        for job in self._items:
            if job['key'] == key:
                # self._sched.unschedule_job(job['start'])
                if job['stop']:
                    self._sched.unschedule_job(job['stop'])
                self._items.remove(job)

    def get_job(self, key=None, name=None):
        self.logger.debug("one job requested")
        one = None
        for job in self._items:
            if job['key'] == key:
                one = dict()
                one['start'] = dict()
                for field in job['start'].trigger.fields:
                    one['start'][field.name] = str(field)
                one['stop'] = dict()
                for field in job['stop'].trigger.fields:
                    one['stop'][field.name] = str(field)
                one['name'] = job['name']
                one['type'] = job['type']
                one['key'] = job['key']
        return one

    def run(self):
        self.initrpc()
        self.init_scheduler()
        self.logger.info("registering with main function")
        sensors = self.rpc.call("domoscore", "register", data=DOMOSTIME_DICT)
        if sensors:
            self.logger.info("Succesfully registered with core")
            for sensor in sensors:
                func = self.domostime_rpc[sensor.pop("rpc")]
                func(**sensor)
        self.logger.info("starting scheduler")
        self._sched.start()
        self.logger.info("starting RPC consumer")
        while not self.done:
            self.rpc.listen()


def start():
    dt = DomosTime()
    dt.start()

def stop():
    pass

def status():
    pass

if __name__ == '__main__':
    dt = DomosTime()
    dt.start()
    dt.join()
