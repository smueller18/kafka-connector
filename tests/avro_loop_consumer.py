import time
import logging

from kafka_connector.avro_loop_consumer import AvroLoopConsumer

LOGGING_FORMAT = "%(levelname)8s %(asctime)s %(name)s [%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


def handle_message(msg):
    print('%s[%d]@%d: key=%s, value=%s' %
          (msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value()))


consumer = AvroLoopConsumer("localhost:9092", "http://localhost:8081", "testgroup", ["testtopic"])
consumer.loop(lambda msg: handle_message(msg))
