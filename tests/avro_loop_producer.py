import time
import datetime as dt
import logging
import os

from kafka_connector.avro_loop_producer import AvroLoopProducer
from kafka_connector.timer import Begin

LOGGING_FORMAT = "%(levelname)8s %(asctime)s %(name)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

__dirname__ = os.path.dirname(os.path.abspath(__file__))


def get_data():
    return {'key': time.time(), 'value': {'name': 'abc', 'number': int(time.time())}}


def get_data2():
    return None

producer = AvroLoopProducer("localhost:9092", "http://localhost:8081", "testtopic",
                            __dirname__ + "/schema/key_schema.avsc",
                            __dirname__ + "/schema/value_schema.avsc")
producer.loop(get_data2)
# producer.loop(get_data, begin=[dt.time(19, 4, 20), dt.time(19, 4, 40)])
