# Copyright (c) 2018-2023 Open Text.
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


from __future__ import print_function, division, absolute_import, annotations

import logging
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Union
from ..os_utils import ensure_dir_exists

class VerticaLogging(object):

    @classmethod
    def setup_logging(cls, logger_name: str, logfile: str,
                      log_level: Union[int, str] = logging.INFO,
                      context: str = '') -> None:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)

        if logfile:
            formatter = logging.Formatter(
                fmt=('%(asctime)s.%(msecs)03d [%(module)s] '
                     '{}/%(process)d:0x%(thread)x <%(levelname)s> '
                     '%(message)s'.format(context)),
                datefmt='%Y-%m-%d %H:%M:%S')
            ensure_dir_exists(logfile)
            file_handler = logging.FileHandler(logfile, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

