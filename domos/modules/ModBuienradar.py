from domos.util.rpc import rpc
from dashi import DashiConnection
import multiprocessing
from datetime import datetime, timedelta, time
import urllib
from apscheduler.schedulers.background import BackgroundScheduler
from collections import namedtuple

BUIENRADAR_URL = "http://gps.buienradar.nl/getrr.php?"
BUIENRADAR_DICT = {
    "name": "BuienRadar",
    "queue": "buienradar",
    "rpc": [
        {"key": "rain_at", "type": "add", "desc": "Expected rain level at minutes in the future", "args": [
            {"name": "lat", "type": "float", "optional": "False", "desc": "Latitude to check for rain"},
            {"name": "lon", "type": "float", "optional": "False", "desc": "longitude to check for rain"},
            {"name": "minutes", "type": "int", "optional": "False", "desc": "minutes in the future, up to 1.5 hours"},
        ]
        },
        {"key": "rain_max", "type": "add", "desc": "Expected rain level at minutes in the future", "args": [
            {"name": "lat", "type": "float", "optional": "False", "desc": "Latitude to check for rain"},
            {"name": "lon", "type": "float", "optional": "False", "desc": "longitude to check for rain"},
            {"name": "minutes", "type": "int", "optional": "False", "desc": "minutes in the future, up to 1.5 hours"},
        ]
        },
    ],
}

Rain = namedtuple("bui", ["key", "lat", "lon", "minute", "func"])


class BuienRadar(multiprocessing.Process):

    def __init__(self):
        """Creates a BuienRadar class
        """

        multiprocessing.Process.__init__(self)
        self.shutdown = False
        self._sched = None
        self._rain = []

    def init_scheduler(self):
        """Initializes the scheduler to poll every five minutes and start it
        """
        self._sched = BackgroundScheduler()
        self._sched.add_job(self._check_rain, trigger='cron', minute='*/5')
        self._sched.start()

    def _check_rain(self):
        print("checking rain")
        for rain in self._rain:
            future_rain = {}
            url = "{0}lat={1}&lon={2}".format(BUIENRADAR_URL, rain.lat, rain.lon)
            with urllib.request.urlopen(url) as f:
                for line in f.read().decode('utf-8').splitlines():
                    rain_level, time_measure = line.split('|')
                    future_rain[time_measure] = int(rain_level)
            print(rain.func(future_rain, rain))

    @staticmethod
    def _calc_rain_mm(level):
        return round(10**((int(level)-109)/32), 3)

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
        rain_mm = BuienRadar._calc_rain_mm(rain_measure[rounded_time])
        print("Rain is expected to be: %.2fmm" % rain_mm, "at", rounded_time)

    def rain_at(self, key=None, lat=None, lon=None, minute=0):
        """

        :param key: A key to distinguish sensors
        :param lat: The latitude to check at
        :param lon: The longitude to check at
        :param minute: The number of minutes in the future to check until
        :return: A three decimal floating point representing the amount of rain in mm/hour at minutes in the future
        """
        print(key, lat, lon, minute)
        if key and lat and lon and minute:
            new_rain = Rain(key, lat, lon, minute, self._at)
            self._rain.append(new_rain)
            return True
        else:
            return False

    def rain_max(self, key=None, lat=None, lon=None, minute=0):
        """Calculate the maximum amount of rain between now and now+minute
        Remote procedure to be called by the core of Domos

        :param key: A key to distinguish sensors
        :param lat: The latitude to check at
        :param lon: The longitude to check at
        :param minute: The number of minutes in the future to check until
        :return: A three decimal floating point representing the maximum amount of rain in mm/hour
        """
        print(key, lat, lon, minute)
        if key and lat and lon and minute:
            new_rain = Rain(key, lat, lon, minute, self._max)
            self._rain.append(new_rain)
            return True
        else:
            return False

    def run(self):
        self.init_scheduler()
        #gps coordinates of the corner of Limburg(NL)
        self.rain_max('1', 50.791026, 5.9689, minute=70)
        while True:
            pass

if __name__ == "__main__":
    print("starting")
    radar = BuienRadar()
    radar.start()
    radar.join()