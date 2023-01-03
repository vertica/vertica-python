# Copyright (c) 2019-2023 Micro Focus or one of its affiliates.
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

from __future__ import print_function, division, absolute_import

import logging
import os

from ...vertica.log import VerticaLogging
from .base import VerticaPythonUnitTestCase


class LoggingTestCase(VerticaPythonUnitTestCase):

    def test_file_handler(self):
        logger_name = "test_file_handler"

        logger = logging.getLogger(logger_name)
        self.assertNotEqual(logging.getLevelName(logger.getEffectiveLevel()), 'DEBUG')

        log_file = os.path.join(self.test_config['log_dir'], 'test_file_handler.log')
        VerticaLogging.setup_logging(logger_name, log_file, 'DEBUG')

        self.assertEqual(len(logger.handlers), 1)
        self.assertEqual(logging.getLevelName(logger.getEffectiveLevel()), 'DEBUG')

    def test_missing_file(self):
        logger_name = "test_missing_file"
        logger = logging.getLogger(logger_name)

        VerticaLogging.setup_logging(logger_name, None, 'DEBUG')
        VerticaLogging.setup_logging(logger_name, '', 'DEBUG')

        self.assertEqual(len(logger.handlers), 0)
        self.assertEqual(logging.getLevelName(logger.getEffectiveLevel()), 'DEBUG')
