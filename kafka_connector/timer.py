# -*- coding: utf-8 -*-

from enum import Enum
import math
import time
import logging
import datetime

__author__ = u'Stephan Müller'
__copyright__ = u'2017, Stephan Müller'
__license__ = u'MIT'

logger = logging.getLogger(__name__)


class Begin(Enum):
    """
    :ivar IMMEDIATELY: 1
    :ivar FULL_CENTISECOND: 2
    :ivar FULL_DECISECOND: 3
    :ivar FULL_SECOND: 4
    :ivar FULL_MINUTE: 5
    :ivar FULL_HOUR: 6
    """
    IMMEDIATELY = 1
    FULL_CENTISECOND = 2
    FULL_DECISECOND = 3
    FULL_SECOND = 4
    FULL_MINUTE = 5
    FULL_HOUR = 6


class Unit(Enum):
    """
    :ivar MILLISECOND: 1
    :ivar SECOND: 2
    :ivar MINUTE: 3
    :ivar HOUR: 4
    """
    MILLISECOND = 1
    SECOND = 2
    MINUTE = 3
    HOUR = 4


class Timer(object):
    """Runs a predefined function every given interval
    """

    def __init__(self, timer_function, interval=1, unit=Unit.SECOND, begin=Begin.FULL_SECOND):

        """

        :param timer_function: function which is called every interval step
        :type timer_function: callable
        :param interval: interval step
        :type interval: int
        :param unit: unit for interval
        :type unit: :class:`Unit`
        :param begin: Set start point. Either choose `None` for starting immediately,
            :class:`kafka_connector.timer.Begin` elements or a list of :class:`datetime.time` including start times.
            In the third case, the start time is set to the time which is the closest from the current timestamp.
        :type begin: None or :class:`Begin` or list of :class:`datetime.time`
        """

        if type(interval) != int:
            raise AttributeError("Interval must be of type int")

        if not (begin is None or isinstance(begin, Begin) or type(begin) is list):
            raise AttributeError("begin must be None, of type <enum 'TimerBegin'> or list")

        elif type(begin) is list:
            for t in begin:
                if type(t) is not datetime.time:
                    raise AttributeError("begin element '" + str(t) + "' is not of type datetime.time")

        if not callable(timer_function):
            raise AttributeError("timer_function is not callable")

        self.timer_function = timer_function
        self.interval = interval
        self.unit = unit
        self.begin = begin

        self._started = False
        self._running = False
        self._stopped = False

    def start(self):
        """
        Start timer and repeat calling :data:`self.timer_function` every :data:`self.interval`
        """

        self._started = True
        self._running = True
        self._stopped = False

        # timestamp for next run in milliseconds
        next_run = None

        if self.begin is None or self.begin == Begin.IMMEDIATELY:
            next_run = time.time()

        elif self.begin == Begin.FULL_CENTISECOND:
            next_run = math.ceil(time.time() * 10) * 100

        elif self.begin == Begin.FULL_DECISECOND:
            next_run = math.ceil(time.time() * 100) * 10

        elif self.begin == Begin.FULL_SECOND:
            next_run = math.ceil(time.time()) * 1000

        elif self.begin == Begin.FULL_MINUTE:
            next_run = math.ceil(time.time() / 60) * 60000

        elif self.begin == Begin.FULL_HOUR:
            next_run = math.ceil(time.time() / 3600) * 3600000

        elif isinstance(self.begin, list):

            date = datetime.datetime.now()
            for i in range(1, 3):

                try:
                    next_run = min(
                            [date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond) for
                             t in self.begin
                             if (date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)
                                 - date).total_seconds() >= 0]
                    ).timestamp() * 1000
                    break
                except ValueError:
                    date = (date + datetime.timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
                    continue

        sleep_time = max(0., next_run / 1000 - time.time())

        if sleep_time > 30:
            logger.info("Going to sleep for " + Timer.str_timedelta(int(sleep_time)))

        time.sleep(max(0., next_run / 1000 - time.time()))

        while self._running:

            try:
                self.timer_function()

            except Exception as e:
                logger.exception(e)

            except KeyboardInterrupt as e:
                raise e

            if self.unit == Unit.MILLISECOND:
                next_run += self.interval

            elif self.unit == Unit.SECOND:
                next_run += self.interval * 1000

            elif self.unit == Unit.MINUTE:
                next_run += self.interval * 60000

            elif self.unit == Unit.HOUR:
                next_run += self.interval * 3600000

            sleep_time = max(0., next_run / 1000 - time.time())

            if sleep_time > 30:
                logger.info("Going to sleep for " + Timer.str_timedelta(int(sleep_time)))

            time.sleep(max(0., next_run / 1000 - time.time()))

        self._stopped = True

    def stop(self):
        """
        Sets loop condition to false and timer loop will break in the next iteration. The current call of the
        :data:`timer_function` will not be aborted.

        """
        self._running = False

    @property
    def is_started(self):
        """
        :return: If the timer has already been started
        :rtype: bool
        """
        return self._started

    @property
    def is_stopped(self):
        """
        :return: If timer loop finished
        :rtype: bool
        """
        return self._stopped

    @staticmethod
    def str_timedelta(seconds):
        """
        Stringify a timedelta from seconds in form x h x min x s
        :param seconds: number of seconds
        :type seconds: int

        :return: timedelta from seconds in form x h x min x s
        :rtype: str
        """

        hour_minute = ""

        if seconds > 3600:
            hour_minute += str(int(seconds / 3600.0)) + " h "

        if seconds > 60:
            hour_minute += str(int(seconds / 60) % 60) + " min "

        return hour_minute + str(seconds % 60) + " s"
