#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, os.path.abspath('../'))
import kafka_connector

import sys
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
