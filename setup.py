#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
import os
import sys
import kafka_connector
from unittest.mock import MagicMock

__author__ = u'Stephan Müller'
__copyright__ = u'2017, Stephan Müller'
__license__ = u'MIT'


class Mock(MagicMock):
    @classmethod
    def __getattr__(cls, name):
            return MagicMock()

MOCK_MODULES = ['confluent_kafka']
sys.modules.update((mod_name, Mock()) for mod_name in MOCK_MODULES)


if sys.version_info[0] < 3:
    avro = 'avro'
else:
    avro = 'avro-python3'

install_requires = [avro]

# prevent readthedocs.org from installing confluent_kafka
if not os.path.dirname(os.path.abspath(__file__)).startswith('/home/docs/checkouts/readthedocs.org/'):
    install_requires.append('confluent_kafka')


setup(
    name='kafka_connector',
    packages=['kafka_connector'],
    version=kafka_connector.__version__,
    license='MIT',
    description='A python module for communication with Kafka.',
    author='Stephan Müller',
    author_email='mail@stephanmueller.eu',
    url='https://github.com/smueller18/kafka-connector',
    download_url='https://github.com/smueller18/kafka-connector',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: MIT License"
    ],
    install_requires=install_requires,
)
