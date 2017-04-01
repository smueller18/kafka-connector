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
    :ivar CUSTOM: 10
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
    """ Timer
    """

    def __init__(self, timer_function, interval=1, unit=Unit.SECOND, begin=Begin.FULL_SECOND):

        """

        :param timer_function: function which is called every interval step
        :type timer_function: callable
        :param interval: interval step
        :type interval: int
        :param unit: unit for interval
        :type unit: :class:`Unit`
        :param begin: set nice start point
        :type begin: :class:`timer.Begin` or list of :class:`datetime.time`
        """

        if type(interval) != int:
            raise AttributeError("Interval must be of type int")

        if not isinstance(begin, Begin):
            if type(begin) is not list:
                raise AttributeError("begin must be of type <enum 'TimerBegin'> or list")
            else:
                for t in begin:
                    if type(t) is not datetime.time:
                        raise AttributeError("begin element '" + str(t) + "' is not of type datetime.time")

        if not callable(timer_function):
            raise AttributeError("timer_function is not callable")

        self._timer_function = timer_function
        self._interval = interval
        self._unit = unit
        self._begin = begin

        self._started = False
        self._running = False
        self._stopped = False

    def start(self):
        """
        Start timer and repeat calling :data:`timer_function` every :data:`interval`
        """

        self._started = True
        self._running = True

        # timestamp for next run in milliseconds
        next_run = None

        if self._begin == Begin.FULL_CENTISECOND:
            next_run = math.ceil(time.time() * 10) * 100

        elif self._begin == Begin.FULL_DECISECOND:
            next_run = math.ceil(time.time() * 100) * 10

        elif self._begin == Begin.FULL_SECOND:
            next_run = math.ceil(time.time()) * 1000

        elif self._begin == Begin.FULL_MINUTE:
            next_run = math.ceil(time.time() / 60) * 60000

        elif self._begin == Begin.FULL_HOUR:
            next_run = math.ceil(time.time() / 3600) * 3600000

        elif isinstance(self._begin, list):

            date = datetime.datetime.now()
            for i in range(0, 2):

                if i > 0:
                    date = (date + datetime.timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
                try:
                    next_run = min(
                            date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond) for
                            t in self._begin
                            if (date.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)
                                - date).total_seconds() > 0
                    ).timestamp() * 1000
                except ValueError:
                    continue

        sleep_time = max(0., next_run / 1000 - time.time())

        if sleep_time > 60:
            logger.info("Going to sleep for " + str(round(sleep_time, 2)) + " seconds")

        time.sleep(max(0., next_run / 1000 - time.time()))

        while self._running:

            try:
                self._timer_function()

                if self._unit == Unit.MILLISECOND:
                    next_run += self._interval

                elif self._unit == Unit.SECOND:
                    next_run += self._interval * 1000

                elif self._unit == Unit.MINUTE:
                    next_run += self._interval * 60000

                elif self._unit == Unit.HOUR:
                    next_run += self._interval * 3600000

                if sleep_time > 60:
                    logger.info("Going to sleep for " + str(round(sleep_time, 2)) + " seconds")

                time.sleep(max(0., next_run / 1000 - time.time()))

            except Exception as e:
                logger.exception(e)

            except KeyboardInterrupt as e:
                raise e

        self._stopped = True

    def stop(self):
        self._running = False

    def is_started(self):
        return self._started

    def is_stopped(self):
        return self._stopped
