#!/usr/bin/env python3

from threading import Thread
import domos.util.domossettings as ds
from domos.util.rpc import rpc
from dashi import DashiConnection
import socket
from multiprocessing import Process
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

ROUTING_KEY = 'sensor.time'
CONTROL_KEY = 'control.time'

DOMOSTIME_DICT = {
    "name": "domosTime",
    "queue": "domosTime",
    "rpc": [
        {"key": "getTimers", "type": "list"},
        {"key": "getTimer", "type": "get", "args":[

            ]
        },
        {"key": "delTimer", "type": "del", "args":[

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


class domosTime(Process):

    def __init__(self):
        Process.__init__(self)
        self.done = False
        self.name = 'domosTime'
        self._items = []
       
    def initScheduler(self):
         self._sched = BackgroundScheduler()
        
    def initrpc(self):
        self.rpc = rpc(self.name)
        self.rpc.handle(self.getJobs, "getTimers")
        self.rpc.handle(self.getJob, "getTimer")
        self.rpc.handle(self.addJob, "addTimer")
        self.rpc.handle(self.delJob, "delTimer")
        self.logmsg('debug', 'Initializing scheduler')
        
    def logmsg(self, level, msg):
        call = 'log_{}'.format(level)
        self.rpc.fire("log", 'log_debug', msg=msg, handle='DomosTime')

    def addJob(self, key=None, 
               name=None, jobtype='Once',
               start=None, stop=None):
        returnvalue = False
        for job in self._items:
            if job['key'] == key:
                #duplicate job found
                break
        else:
            #no duplicates found
            newjob = dict()
            if stop:
                #True job
                newjob['start'] = self._sched.add_job(self._jobTrue,
                                                      args=[key, name],
                                                      trigger='cron',
                                                      name=name,
                                                      **start)
                #false job
                newjob['stop'] = self._sched.add_job(self._jobFalse,
                                                     args=[key, name],
                                                     trigger='cron',
                                                     name=name,
                                                     **stop)
                newjob['name'] = name
                newjob['key'] = key
                newjob['type'] = jobtype
                returnvalue = True
            else:
                newjob['start'] = self._sched.add_job(self._jobOnce,
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

    def _jobTrue(self, key, name):
        self.rpc.fire("domoscore", 'sensorValue', key=key, value='1')

    def _jobFalse(self, key, name):
        self.rpc.fire("domoscore", 'sensorValue', key=key, value='0')

    def _jobOnce(self, key, name):
        self.rpc.fire("domoscore", 'sensorValue', key=key, value='1')

    def getJobs(self):
        self.rpc.log_debug("All jobs requested")
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

    def delJob(self, key=None):
        self.rpc.log_debug("one job deletion requested")
        for job in self._items:
            if job['key'] == key:
                #self._sched.unschedule_job(job['start'])
                if job['stop']:
                    self._sched.unschedule_job(job['stop'])
                self._items.remove(job)

    def getJob(self, key=None, name=None):
        self.rpc.log_debug("one job requested")
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
        self.initScheduler()
        self.rpc.log_info("registering with main function")
        sensors = self.rpc.call("domoscore", "register", data=DOMOSTIME_DICT)
        if sensors:
            self.rpc.log_info("Succesfully registered with core")
            for sensor in sensors:
                self.addJob(**sensor)
        self.rpc.log_info("starting scheduler")
        self._sched.start()
        self.rpc.log_info("starting RPC consumer")
        while not self.done:
            self.rpc.listen()

if __name__ == '__main__':
    dt = domosTime()
    dt.start()
    dt.join()
