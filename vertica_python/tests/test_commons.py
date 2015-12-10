import unittest
import os
import vertica_python

conn_info = {'host': os.getenv('VP_TEST_HOST', '127.0.0.1'),
             'port': int(os.getenv('VP_TEST_PORT', 5433)),
             'user': os.getenv('VP_TEST_USER', 'dbadmin'),
             'password': os.getenv('VP_TEST_PASSWD', ''),
             'database': os.getenv('VP_TEST_DB', 'docker')}


class VerticaTestCase(unittest.TestCase):
    """
    Base class for tests that query Vertica.

    Implements a couple of functions for mindless repetetive tasks.
    """
    def query_and_fetchall(self, query):
        """
        Creates new connection to vertica, executes query, returns all fetched results. Closes connection.
        :param query: query to execute
        :return: all fetched results as returned by cursor.fetchall()
        """
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(query)

            return cur.fetchall()
