#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

from mock import Mock

confluent_kafka_mock = Mock()
confluent_kafka_mock_avro = Mock(AvroProducer=object, AvroConsumer=object)

sys.modules.update([("confluent_kafka", confluent_kafka_mock)])
sys.modules.update([("confluent_kafka.avro", confluent_kafka_mock_avro)])

sys.path.insert(0, os.path.abspath('../'))
import kafka_connector


__author__ = u'Stephan Müller'
__copyright__ = u'2017, Stephan Müller'
__license__ = u'MIT'


version = release = kafka_connector.__version__

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.coverage',
    'sphinx.ext.intersphinx',
]

templates_path = ['_templates']
exclude_patterns = ['_build']
html_static_path = ['_static']

source_suffix = '.rst'
master_doc = 'index'

html_theme = 'sphinx_rtd_theme'
pygments_style = 'sphinx'
htmlhelp_basename = 'kafka-connector'

autodoc_default_flags = ['special-members', 'private-members', 'show-inheritance']

# objects.inv
intersphinx_mapping = {
    'python': ('https://docs.python.org/3.5', None),
    'confluent_kafka': ('http://docs.confluent.io/3.0.0/clients/confluent-kafka-python/', None)
}
