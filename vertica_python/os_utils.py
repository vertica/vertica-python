# Copyright (c) 2020-2023 Micro Focus or one of its affiliates.
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

import errno
import os


def ensure_dir_exists(filepath):
    """Ensure that a directory exists

    If it doesn't exist, try to create it and protect against a race condition
    if another process is doing the same.
    """
    directory = os.path.dirname(filepath)
    if directory != '' and not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

def check_file_readable(filename):
    """Ensure this is a readable file"""
    if not os.path.exists(filename):
        raise OSError('{} does not exist'.format(filename))
    elif not os.path.isfile(filename):
        raise OSError('{} is not a file'.format(filename))
    elif not os.access(filename, os.R_OK):
        raise OSError('{} is not readable'.format(filename))

def check_file_writable(filename):
    """Ensure this is a writable file. If the file doesn't exist,
       ensure its directory is writable.
    """
    if os.path.exists(filename):
        if not os.path.isfile(filename):
            raise OSError('{} is not a file'.format(filename))
        if not os.access(filename, os.W_OK):
            raise OSError('{} is not writable'.format(filename))
    # If target does not exist, check permission on parent dir
    ensure_dir_exists(filename)
    pdir = os.path.dirname(filename)
    if not pdir:
        pdir = '.'
    if not os.path.isdir(pdir):
        raise OSError('{} is not a directory'.format(pdir))
    if not os.access(pdir, os.W_OK):
        raise OSError('Directory {} is not writable'.format(pdir))

