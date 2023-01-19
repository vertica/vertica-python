# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
# Copyright (c) 2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright (c) 2013-2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import print_function, division, absolute_import

import os
import sys
import logging
import unittest
import inspect
import getpass
from configparser import ConfigParser

from ...compat import as_text, as_str, as_bytes
from ...vertica.log import VerticaLogging


default_configs = {
    'log_dir': 'vp_test_log',
    'log_level': logging.WARNING,
    'host': 'localhost',
    'port': 5433,
    'user': getpass.getuser(),
    'password': '',
}


class VerticaPythonTestCase(unittest.TestCase):
    """
    Base class for all tests
    """

    @classmethod
    def _load_test_config(cls, config_list):
        test_config = {}

        # load default configurations
        for key in config_list:
            if key != 'database':
                test_config[key] = default_configs[key]

        # override with the configuration file
        confparser = ConfigParser()
        confparser.optionxform = str
        SECTION = 'vp_test_config'  # section name in the configuration file
        # the configuration file is placed in the same directory as this file
        conf_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 'vp_test.conf')
        confparser.read(conf_file)
        for k in config_list:
            option = 'VP_TEST_' + k.upper()
            if confparser.has_option(SECTION, option):
                test_config[k] = confparser.get(SECTION, option)

        # override again with VP_TEST_* environment variables
        for k in config_list:
            env = 'VP_TEST_' + k.upper()
            if env in os.environ:
                test_config[k] = os.environ[env]

        # data preprocessing
        # value is string when loaded from configuration file and environment variable
        if 'port' in test_config:
            test_config['port'] = int(test_config['port'])
        if 'database' in config_list and 'user' in test_config:
            test_config.setdefault('database', test_config['user'])
        if 'log_level' in test_config:
            levels = ['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if isinstance(test_config['log_level'], str):
                if test_config['log_level'] not in levels:
                    raise ValueError("Invalid value for VP_TEST_LOG_LEVEL: '{}'".format(test_config['log_level']))
                test_config['log_level'] = eval('logging.' + test_config['log_level'])
        if 'log_dir' in test_config:
            test_config['log_dir'] = os.path.join(test_config['log_dir'],
                                     'py{0}{1}'.format(sys.version_info.major, sys.version_info.minor))

        return test_config

    @classmethod
    def _setup_logger(cls, tag, log_dir, log_level):
        # Setup test logger
        # E.g. If the class is defined in tests/integration_tests/test_dates.py
        #      and test cases run under python3.7, then
        #      the log would write to $VP_TEST_LOG_DIR/py37/integration_tests/test_dates.log

        testfile = os.path.splitext(os.path.basename(inspect.getsourcefile(cls)))[0]
        logfile = os.path.join(log_dir, tag, testfile + '.log')
        VerticaLogging.setup_logging(cls.__name__, logfile, log_level, cls.__name__)
        cls.logger = logging.getLogger(cls.__name__)
        return logfile

    def setUp(self):
        self.logger.info('\n\n'+'-'*50+'\n Begin '+self.__class__.__name__+"."+self._testMethodName+'\n'+'-'*50)

    def tearDown(self):
        self.logger.info('\n'+'-'*10+' End '+self.__class__.__name__+"."+self._testMethodName+' '+'-'*10+'\n')

    # Common assertions
    def assertTextEqual(self, first, second, msg=None):
        first_text = as_text(first)
        second_text = as_text(second)
        self.assertEqual(first=first_text, second=second_text, msg=msg)

    def assertStrEqual(self, first, second, msg=None):
        first_str = as_str(first)
        second_str = as_str(second)
        self.assertEqual(first=first_str, second=second_str, msg=msg)

    def assertBytesEqual(self, first, second, msg=None):
        first_bytes = as_bytes(first)
        second_bytes = as_bytes(second)
        self.assertEqual(first=first_bytes, second=second_bytes, msg=msg)

    def assertResultEqual(self, value, result, msg=None):
        if isinstance(value, str):
            self.assertTextEqual(first=value, second=result, msg=msg)
        else:
            self.assertEqual(first=value, second=result, msg=msg)

    def assertListOfListsEqual(self, list1, list2, msg=None):
        self.assertEqual(len(list1), len(list2), msg=msg)
        for l1, l2 in zip(list1, list2):
            self.assertListEqual(l1, l2, msg=msg)
