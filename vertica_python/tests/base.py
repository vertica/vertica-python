from __future__ import print_function, division, absolute_import

import os
import unittest

from six import string_types

from .. import *
from ..compat import as_text, as_str, as_bytes

DEFAULT_VP_TEST_HOST = '127.0.0.1'
DEFAULT_VP_TEST_PORT = 5433
DEFAULT_VP_TEST_USER = 'dbadmin'
DEFAULT_VP_TEST_PASSWD = ''
DEFAULT_VP_TEST_DB = 'docker'
DEFAULT_VP_TEST_TABLE = 'vertica_python_unit_test'


class VerticaPythonTestCase(unittest.TestCase):
    """Base class for tests that query Vertica."""

    @classmethod
    def setUpClass(cls):
        cls._host = os.getenv('VP_TEST_HOST', DEFAULT_VP_TEST_HOST)
        cls._port = int(os.getenv('VP_TEST_PORT', DEFAULT_VP_TEST_PORT))
        cls._user = os.getenv('VP_TEST_USER', DEFAULT_VP_TEST_USER)
        cls._password = os.getenv('VP_TEST_PASSWD', DEFAULT_VP_TEST_PASSWD)
        cls._database = os.getenv('VP_TEST_DB', DEFAULT_VP_TEST_DB)
        cls._table = os.getenv('VP_TEST_TABLE', DEFAULT_VP_TEST_TABLE)

        cls._conn_info = {
            'host': cls._host,
            'port': cls._port,
            'database': cls._database,
            'user': cls._user,
            'password': cls._password,
        }

    @classmethod
    def tearDownClass(cls):
        with cls._connect() as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS {0}".format(cls._table))

    @classmethod
    def _connect(cls):
        """Connects to vertica.
        
        :return: a connection to vertica.
        """
        return connect(**cls._conn_info)

    def _query_and_fetchall(self, query):
        """Creates a new connection, executes a query and fetches all the results.
        
        :param query: query to execute
        :return: all fetched results as returned by cursor.fetchall()
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()

        return results

    def _query_and_fetchone(self, query):
        """Creates a new connection, executes a query and fetches one result.
        
        :param query: query to execute
        :return: the first result fetched by cursor.fetchone()
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()

        return result

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
        if isinstance(value, string_types):
            self.assertTextEqual(first=value, second=result, msg=msg)
        else:
            self.assertEqual(first=value, second=result, msg=msg)

    def assertListOfListsEqual(self, list1, list2, msg=None):
        self.assertEqual(len(list1), len(list2), msg=msg)
        for l1, l2 in zip(list1, list2):
            self.assertListEqual(l1, l2, msg=msg)
