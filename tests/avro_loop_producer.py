import time
import datetime as dt
import logging

from kafka_connector.avro_loop_producer import AvroLoopProducer
from kafka_connector.timer import Begin

LOGGING_FORMAT = "%(levelname)8s %(asctime)s %(name)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


def get_data():
    return {'key': time.time(), 'value': {'name': 'abc '}}


stcs = AvroLoopProducer("localhost:9092", "http://localhost:8081", "testtopic",
                        "schema/key_schema.avsc", "schema/value_schema.avsc")
stcs.loop(get_data)
# stcs.loop(get_data, begin=[dt.time(19, 4, 20), dt.time(19, 4, 40)])
