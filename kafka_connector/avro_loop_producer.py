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

default_conf = {
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

    """
    
    The default config is
    
    >>> default_conf = {    
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

    def __init__(self, bootstrap_servers, schema_registry_url, topic, key_schema, value_schema,
                 config=default_conf, error_callback=lambda err: AvroLoopProducer.error_callback(err)):
        """

        :param bootstrap_servers: 
        :type bootstrap_servers: str
        :param schema_registry_url: 
        :type schema_registry_url: str
        :param topic: topic name
        :type topic: str
        :param key_schema: Avro schema for key
        :type key_schema: str
        :param value_schema: Avro schema for value
        :type value_schema: str
        :param config: 
        :type config: dict
        :param error_callback: function that handles occurring error events
        :type error_callback: lambda err: function(err)

        :raise SchemaParseException: 
        """

        self._timer = None

        self._topic = topic
        self._config = config

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

    def produce(self, key, value, timestamp=None, on_delivery=lambda err, msg: AvroLoopProducer.on_delivery(err, msg),
                poll_timeout=0.01, **kwargs):
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
        :param poll_timeout: If timeout is a number or `None`: Polls the producer for events and calls the corresponding 
            callbacks (if registered). On `False` do not call :func:`confluent_kafka.Producer.poll(timeout)`.
        :type poll_timeout: None, float
        
        :raises BufferError: if the internal producer message queue is full (``queue.buffering.max.messages`` exceeded)
        :raises ~confluent_kafka.KafkaException: for other errors, see exception code
        :raises NotImplementedError: if timestamp is specified without underlying library support.
        :raises SerializerError:     
        :raises avro.schema.SchemaParseException:
        """

        if on_delivery is not None:
            kwargs.update({"on_delivery": on_delivery})

        if timestamp is not None:
            kwargs.update({"timestamp": timestamp})

        try:
            super().produce(topic=self._topic, key=key, value=value, **kwargs)
        except requests.exceptions.ConnectionError as e:
            logger.error(e)
            time.sleep(1)

        if type(poll_timeout) != bool:
            super().poll(timeout=poll_timeout)

    def loop(self, get_data_function, interval=1, unit=Unit.SECOND, begin=Begin.FULL_SECOND):

        get_data_kwargs = get_data_function()

        self._timer = Timer(lambda **kwargs: self.produce(**get_data_kwargs), interval, unit, begin)
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
        if err is not None:
            logger.error(str(err))
        else:
            logger.info("Delivered message with offset " + str(msg.offset()) + " successfully")

    @staticmethod
    def error_callback(err):
        logger.error(str(err))
