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


# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""Functions for Python 2 vs. 3 compatibility.
## Conversion routines
In addition to the functions below, `as_str` converts an object to a `str`.
@@as_bytes
@@as_text
@@as_str_any
## Types
The compatibility module also provides the following types:
* `bytes_or_text_types`
* `complex_types`
* `integral_types`
* `real_types`
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six as _six


def as_bytes(bytes_or_text, encoding='utf-8'):
    """Converts either bytes or unicode to `bytes`, using utf-8 encoding for text.
    Args:
      bytes_or_text: A `bytes`, `bytearray`, `str`, or `unicode` object.
      encoding: A string indicating the charset for encoding unicode.
    Returns:
      A `bytes` object.
    Raises:
      TypeError: If `bytes_or_text` is not a binary or unicode string.
    """
    if isinstance(bytes_or_text, _six.text_type):
        return bytes_or_text.encode(encoding)
    elif isinstance(bytes_or_text, bytearray):
        return bytes(bytes_or_text)
    elif isinstance(bytes_or_text, bytes):
        return bytes_or_text
    else:
        raise TypeError('Expected binary or unicode string, got %r' %
                        (bytes_or_text,))


def as_text(bytes_or_text, encoding='utf-8'):
    """Returns the given argument as a unicode string.
    Args:
      bytes_or_text: A `bytes`, `bytearray`, `str`, or `unicode` object.
      encoding: A string indicating the charset for decoding unicode.
    Returns:
      A `unicode` (Python 2) or `str` (Python 3) object.
    Raises:
      TypeError: If `bytes_or_text` is not a binary or unicode string.
    """
    if isinstance(bytes_or_text, _six.text_type):
        return bytes_or_text
    elif isinstance(bytes_or_text, (bytes, bytearray)):
        return bytes_or_text.decode(encoding)
    else:
        raise TypeError('Expected binary or unicode string, got %r' % bytes_or_text)


# Convert an object to a `str` in both Python 2 and 3.
if _six.PY2:
    as_str = as_bytes
else:
    as_str = as_text


def as_str_any(value):
    """Converts to `str` as `str(value)`, but use `as_str` for `bytes`.
    Args:
      value: A object that can be converted to `str`.
    Returns:
      A `str` object.
    """
    if isinstance(value, (bytes, bytearray)):
        return as_str(value)
    else:
        return str(value)


# Either bytes or text.
bytes_or_text_types = (bytes, bytearray, _six.text_type)

_allowed_symbols = [
    'as_str',
    'bytes_or_text_types',
]
