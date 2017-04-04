# -*- coding: utf-8 -*-

import logging
import time
import requests.exceptions

from avro.schema import SchemaParseException
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from kafka_connector.timer import Timer, Begin, Unit

__author__ = u'Stephan Müller'
__copyright__ = u'2017, Stephan Müller'
__license__ = u'MIT'

logger = logging.getLogger(__name__)

default_config = {
    'log_level': 0,
    'api.version.request': True,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 10,
    'message.send.max.retries': 200,
    'default.topic.config':
        {
            'produce.offset.report': True
        }
}


class AvroLoopProducer(AvroProducer):

    """AvroProducer with integrated timer function that calls a data producing function every defined interval.

    The default config is

    >>> default_config = {
    ...    'log_level': 0,
    ...    'api.version.request': True,
    ...    'queue.buffering.max.messages': 100000,
    ...    'queue.buffering.max.ms': 10,
    ...    'message.send.max.retries': 200,
    ...    'default.topic.config':
    ...      {
    ...        'produce.offset.report': True
    ...      }
    ...  }

    """

    def __init__(self, bootstrap_servers, schema_registry_url, topic, key_schema, value_schema, poll_timeout=0.01,
                 config=default_config, error_callback=lambda err: AvroLoopProducer.error_callback(err)):
        """

        :param bootstrap_servers: Initial list of brokers as a CSV list of broker host or host:port.
        :type bootstrap_servers: str
        :param schema_registry_url: url for schema registry
        :type schema_registry_url: str
        :param topic: topic name
        :type topic: str
        :param key_schema: Avro schema for key
        :type key_schema: str
        :param value_schema: Avro schema for value
        :type value_schema: str
        :param poll_timeout: If timeout is a number or `None`: Polls the producer for events and calls the corresponding
            callbacks (if registered). On `False` do not call :func:`confluent_kafka.Producer.poll(timeout)`.
        :type poll_timeout: None, float
        :param config: A config dictionary with properties listed at
            https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        :type config: dict
        :param error_callback: function that handles occurring error events
        :type error_callback: lambda err: function(err)

        :raises avro.schema.SchemaParseException: if either key or value schema is invalid
        """

        self._timer = None

        self._topic = topic
        self._config = config
        self._poll_timeout = poll_timeout

        if error_callback is not None:
            self._config.update({"error_cb": error_callback})

        self._config['bootstrap.servers'] = bootstrap_servers
        self._config['schema.registry.url'] = schema_registry_url

        try:
            self._key_schema = avro.load(key_schema)
        except SchemaParseException:
            raise SchemaParseException("Invalid Avro schema for key")

        try:
            self._value_schema = avro.load(value_schema)
        except SchemaParseException:
            raise SchemaParseException("Invalid Avro schema for value")

        super().__init__(self._config, default_key_schema=self._key_schema, default_value_schema=self._value_schema)

    def produce(self, key=None, value=None, partition=None, timestamp=None,
                on_delivery=lambda err, msg: AvroLoopProducer.on_delivery(err, msg)):
        """
        Sends message to kafka by encoding with specified avro schema

        :param key: An object to serialize
        :type key: any
        :param value: An object to serialize
        :type key: any
        :param timestamp: Message timestamp (CreateTime) in microseconds since epoch UTC (requires librdkafka >= v0.9.4,
            api.version.request=true, and broker >= 0.10.0.0). Default value is current time.
        :param on_delivery: callbacks from :func:`produce()`
        :type on_delivery: lambda err, msg

        :raises BufferError: if the internal producer message queue is full (``queue.buffering.max.messages`` exceeded)
        :raises ~confluent_kafka.KafkaException: see exception code
        :raises NotImplementedError: if timestamp is specified without underlying library support.
        :raises avro.schema.SchemaParseException: schema is not a valid Avro schema
        """

        kwargs = dict()

        if key is not None:
            kwargs['key'] = key

        if value is not None:
            kwargs['value'] = value

        if partition is not None:
            kwargs['partition'] = partition

        if timestamp is not None:
            kwargs.update({"timestamp": timestamp})

        if on_delivery is not None:
            kwargs.update({"on_delivery": on_delivery})

        try:
            super().produce(topic=self._topic, **kwargs)

        # if connection to schema registry server is down
        except requests.exceptions.ConnectionError as e:
            logger.error(e)
            time.sleep(1)

        if type(self._poll_timeout) != bool:
            super().poll(timeout=self._poll_timeout)

    def _loop_produce(self, data_function):
        """
        Preprocess data_function. Only allow valid results being pushed to Kafka.

        :param data_function:
        :type data_function:
        """

        data = data_function()
        if data is None:
            logger.warning("The result of data_function is None. Continue without sending any message.")

        elif type(data) is not dict:
            logger.warning("The result of data_function is not a dictionary. Continue without sending any message.")

        elif 'key' not in data and 'value' not in data and 'timestamp' not in data:
            logger.warning("The result of data_function does not contain any elements of 'key', 'value' or 'timestamp'."
                           "Continue without sending any message.")

        else:
            self.produce(**data)

    def loop(self, data_function, interval=1, unit=Unit.SECOND, begin=Begin.FULL_SECOND):
        """
        Start timer that calls :data:`data_function` every defined interval.

        :param data_function: the result of this function is used as ``**kwargs`` for :meth:`produce()`
        :type data_function: function that returns a dict with possible keys `key`, `value`, `timestamp`, `partition`
            and `on_delivery`
        :param interval: interval step
        :type interval: int
        :param unit: unit for interval
        :type unit: :class:`~kafka_connector.timer.Unit`
        :param begin: Set start point. Either choose one of :class:`kafka_connector.timer.Begin` elements or a list of
            :class:`datetime.time` including start times. In the second case, the start time is set to the time which is
            the closest from the current timestamp.
        :type begin: :class:`kafka_connector.timer.Begin` or list of :class:`datetime.time`
        """
        self._timer = Timer(lambda: self._loop_produce(data_function), interval, unit, begin)
        try:
            self._timer.start()
        except KeyboardInterrupt:
            super().flush(0.1)
            # todo handle KeyboardInterrupt
            return

    def stop(self):
        """
        Stops the timer if it is running
        """
        if self._timer is not None and not self._timer.is_stopped():
            self._timer.stop()

    @staticmethod
    def on_delivery(err, msg):
        """
        Handles callbacks from :func:`produce()`
        """
        if err is not None:
            logger.error(str(err))
        else:
            logger.info("Delivered message with offset " + str(msg.offset()) + " successfully")

    @staticmethod
    def error_callback(err):
        """
        Handles error message
        """
        logger.error(str(err))
