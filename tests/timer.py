from kafka_connector.timer import Timer
import time
import datetime
import logging


LOGGING_FORMAT = "%(levelname)8s %(asctime)s %(name)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


begin_times = [
    datetime.time(hour=0),
    datetime.time(hour=6),
    datetime.time(hour=12),
    datetime.time(hour=18)
]


def func():
    print(time.time())


timer = Timer(func, begin=begin_times)
timer.start()
