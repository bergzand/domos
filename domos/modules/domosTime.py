#!/usr/bin/env python3

from threading import Thread
import domos.util.domossettings as ds
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


class domosTime(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.done = False
        self.name = 'domosTime'
        self._items = []
        dashiconfig = ds.domosSettings.getDashiConfig()
        self.dashi = DashiConnection(self.name, dashiconfig["amqp_uri"], dashiconfig['exchange'], sysname = dashiconfig['sysname'])
        self._sched = BackgroundScheduler()
        self.logmsg('debug','Initializing scheduler')
        self.dashi.handle(self.getJobs, "getTimers")
        self.dashi.handle(self.getJob, "getTimer")
        self.dashi.handle(self.addJob, "addTimer")
        self.dashi.handle(self.delJob, "delTimer")
        #self.addJob('test', 'test1', 'Toggle', {'second': '10,30,50'}, {'second':'0,20,40'})
        
    def logmsg(self, level, msg):
        call = 'log_{}'.format(level)
        self.dashi.fire("log", 'log_debug', msg=msg, handle='DomosTime')


    def addJob(self, key=None, ident=None, jobtype='Once', start=None, stop=None):
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
                newjob['start'] = self._sched.add_job(self._jobTrue, args=[key, ident], trigger='cron', name=ident, **start)
                #false job
                newjob['stop'] = self._sched.add_job(self._jobFalse, args=[key, ident], trigger='cron', name=ident, **stop)
                newjob['ident'] = ident
                newjob['key'] = key
                newjob['type'] = jobtype
                returnvalue = True
            else:
                newjob['start'] = self._sched.add_job(self._jobOnce, args=[key, ident], trigger='cron', name=ident, **start)
                newjob['stop'] = None
                newjob['ident'] = ident
                newjob['key'] = key
                newjob['type'] = jobtype
                returnvalue = True
            self._items.append(newjob)
        return returnvalue

    def _jobTrue(self, key, ident):
        self.dashi.fire("domoscore", 'sensorValue', data={'key':key, 'ident': ident, 'value': '1'})

    def _jobFalse(self, key, ident):
        self.dashi.fire("domoscore", 'sensorValue', data={'key':key, 'ident': ident, 'value': '0'})

    def _jobOnce(self, key, ident):
        self.dashi.fire("domoscore", 'sensorValue', data={'key':key, 'ident': ident, 'value': '1'})
    
    def getJobs(self):
        self.logmsg("debug", "All jobs requested")
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
            one['ident'] = job['ident']
            one['type'] = job['type']
            one['key'] = job['key']
            jobs.append(one)
        return jobs
    
    def delJob(self, key=None):
        self.logmsg("debug", "one job deletion requested")
        for job in self._items:
            if job['key'] == key:
                #self._sched.unschedule_job(job['start'])
                if job['stop']:
                    self._sched.unschedule_job(job['stop'])
                self._items.remove(job)
                
    def getJob(self, key=None, ident=None):
        self.logmsg("debug", "one job requested")
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
                one['ident'] = job['ident']
                one['type'] = job['type']
                one['key'] = job['key']
        return one

    def prerun(self):
        pass

    def cleanup(self):
        pass
        
    
    def run(self):
        self.logmsg("info", "registering with main function")
        if self.dashi.call("domoscore", "register", data=DOMOSTIME_DICT):
            self.logmsg("info", "Succesfully registered with core")
        self.logmsg("info", "starting scheduler")
        self._sched.start()
        self.logmsg("info", "starting Dashi consumer")
        while not self.done:
            try:
                self.dashi.consume(timeout=2)
            except socket.timeout as ex:
                pass

if __name__ == '__main__':
    dt = domosTime()
    dt.start()
    dt.join()
