import urllib.request
from domos.util.rpc import rpc
from domos.util.domoslog import rpchandler
import logging
import multiprocessing
import threading
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from collections import namedtuple
from domos.util.domossettings import domosSettings

BUIENRADAR_URL = "http://gps.buienradar.nl/getrr.php?"
BUIENRADAR_DICT = {
    "name": "BuienRadar",
    "queue": "buienradar",
    "rpc": [
        {"key": "rain_at", "type": "add", "desc": "Expected rain level at minutes in the future", "args": [
            {"name": "lat", "type": "float", "optional": "False", "desc": "Latitude to check for rain"},
            {"name": "lon", "type": "float", "optional": "False", "desc": "longitude to check for rain"},
            {"name": "minute", "type": "int", "optional": "False", "desc": "minutes in the future, up to 1.5 hours"},
        ]
        },
        {"key": "rain_max", "type": "add", "desc": "Expected rain level at minutes in the future", "args": [
            {"name": "lat", "type": "float", "optional": "False", "desc": "Latitude to check for rain"},
            {"name": "lon", "type": "float", "optional": "False", "desc": "longitude to check for rain"},
            {"name": "minute", "type": "int", "optional": "False", "desc": "minutes in the future, up to 1.5 hours"},
        ]
        },
    ],
}

Rain = namedtuple("bui", ["key", "lat", "lon", "minute", "func"])


class BuienRadar(multiprocessing.Process):

    def __init__(self):
        """Creates a BuienRadar class
        """
        self.buienradar_rpc = {"rain_at": self.rain_at,
                               "rain_max": self.rain_max
                               }

        multiprocessing.Process.__init__(self)
        self.name = 'buienradar'
        self.shutdown = False
        self._sched = None
        self._rain = []

    def _init_rpc(self):
        self.rpc = rpc(self.name)

        self.logger = logging.getLogger('BuienRadar')
        loghandler = rpchandler(self.rpc)
        self.logger.addHandler(loghandler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(domosSettings.getLoggingLevel('BuienRadar'))


        for call, func in self.buienradar_rpc.items():
            self.rpc.handle(func, call)
        self.logger.debug('Initializing scheduler')

    def _init_scheduler(self):
        """Initializes the scheduler to poll every five minutes and start it
        """
        self._sched = BackgroundScheduler()
        self._sched.add_job(self._check_rain, trigger='cron', minute='*/5')
        self._sched.start()

    def _register(self):
        sensors = self.rpc.call("domoscore", "register", data=BUIENRADAR_DICT)
        if sensors:
            for sensor in sensors:
                func = self.buienradar_rpc[sensor.pop("rpc")]
                func(**sensor)
        self.logger.debug("Registered with core")


    def _check_rain(self):
        self.logger.debug("checking rain")
        for rain in self._rain:
            future_rain = {}
            url = "{0}lat={1}&lon={2}".format(BUIENRADAR_URL, rain.lat, rain.lon)
            with urllib.request.urlopen(url) as f:
                for line in f.read().decode('utf-8').splitlines():
                    rain_level, time_measure = line.split('|')
                    future_rain[time_measure] = int(rain_level)
            value = rain.func(future_rain, rain)
            self.rpc.fire("domoscore", "sensorValue", key=rain.key, value=value)

    @staticmethod
    def _calc_rain_mm(level):
        return round(10**((int(level) - 109) / 32), 3)

    def _max(self, rain_measure, rain):
        """

        :param rain: dict with level of rain per timestamp
        :return: The maximum rain found between now and the given minutes in the future
        """
        max_rain = 0
        now = datetime.now()
        needed_datetime = now + timedelta(minutes=rain.minute)
        for rain_time, rain_level in rain_measure.items():
            if max_rain < rain_level:
                rain_time = datetime.strptime(rain_time, '%H:%M')
                rain_datetime = datetime(now.year,
                                         now.month,
                                         now.day,
                                         rain_time.hour,
                                         rain_time.minute)
                if rain_datetime < now:
                    rain_datetime += timedelta(days=1)
                if rain_datetime < needed_datetime:
                    max_rain = rain_level

        return BuienRadar._calc_rain_mm(max_rain)

    def _at(self, rain_measure, rain):
        needed_datetime = datetime.now() + timedelta(minutes=rain.minute)
        rounded_datetime = needed_datetime - timedelta(minutes=needed_datetime.minute % 5,
                                                       seconds=needed_datetime.second,
                                                       microseconds=needed_datetime.microsecond)
        rounded_time = rounded_datetime.strftime('%H:%M')
        return BuienRadar._calc_rain_mm(rain_measure[rounded_time])

    def rain_at(self, key=None, name=None, lat=None, lon=None, minute=0):
        """

        :param key: A key to distinguish sensors
        :param lat: The latitude to check at
        :param lon: The longitude to check at
        :param minute: The number of minutes in the future to check until
        :return: A three decimal floating point representing the amount of rain in mm/hour at minutes in the future
        """
        self.logger.info("added sensor for rain at %s : %s for %s minutes" % (lat, lon, minute))
        if not (key and lat and lon and minute):
            return False
        try:
            minute = int(minute)
        except:
            return False
        new_rain = Rain(key, lat, lon, minute, self._at)
        self._rain.append(new_rain)
        return True

    def rain_max(self, key=None, name=None, lat=None, lon=None, minute=0):
        """Calculate the maximum amount of rain between now and now+minute
        Remote procedure to be called by the core of Domos

        :param key: A key to distinguish sensors
        :param lat: The latitude to check at
        :param lon: The longitude to check at
        :param minute: The number of minutes in the future to check until
        :return: A three decimal floating point representing the maximum amount of rain in mm/hour
        """
        self.logger.info("added sensor for rain max %s : %s for %s minutes" % (lat, lon, minute))
        if key and lat and lon and minute:
            try:
                minute = int(minute)
            except:
                return False
            new_rain = Rain(key, lat, lon, minute, self._max)
            self._rain.append(new_rain)
            return True
        else:
            return False

    def run(self):
        self._init_rpc()
        self._init_scheduler()
        self._register()
        self.logger.info("starting mod_buienradar")
        #gps coordinates of north France
        while True:
            self.rpc.listen()

def start():
    br = BuienRadar()
    br.start()



if __name__ == "__main__":
    print("starting")
    radar = BuienRadar()
    radar.start()
    radar.join()