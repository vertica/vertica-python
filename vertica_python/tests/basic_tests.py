import unittest
import logging

import vertica_python
from vertica_python import errors

conn_info = {'host': '127.0.0.1', 
             'port': 5433, 
             'user': 'dbadmin', 
             'password': 'unit_test', 
             'database': 'unit_test'}
logger = logging.getLogger('vertica')


def init_table(cur):
    # clean old table
    cur.execute('DROP TABLE IF EXISTS vertica_python_unit_test;')
    
    # create test table
    cur.execute("""CREATE TABLE vertica_python_unit_test (
                    a int,
                    b varchar(32)
                    ) ;
                """)

class TestVerticaPython(unittest.TestCase):

    def test_inline_commit(self):

        conn = vertica_python.connect(conn_info)
        cur = conn.cursor()
        init_table(cur)
        
        cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (1, 'aa'); commit; """)
        cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 1")
        res = cur.fetchall()
        assert 1 == len(res)
        assert 1 == res[0][0]
        assert 'aa' == res[0][1]

    def test_multi_inserts_and_transaction(self):

        conn = vertica_python.connect(conn_info)
        cur = conn.cursor()
        init_table(cur)
    
        conn2 = vertica_python.connect(conn_info)
        cur2 = conn2.cursor()
    
        # insert data without a commit
        cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (2, 'bb') """)
    
        # verify we can see it from this cursor
        cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 2")
        res = cur.fetchall()
        assert 1 == len(res)
        assert 2 == res[0][0]
        assert 'bb' == res[0][1]
        
        # verify we cant see it from other cursor
        cur2.execute("SELECT a, b from vertica_python_unit_test WHERE a = 2")
        res = cur2.fetchall()
        assert 0 == len(res)
    
        # insert more data then commit
        cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (3, 'cc') """)
        cur.execute(""" commit; """)
        
        # verify we can see it from this cursor
        cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 2 or a = 3")
        res = cur.fetchall()
        assert 2 == len(res)
    
        # verify we can see it from other cursor
        cur2.execute("SELECT a, b from vertica_python_unit_test WHERE a = 2 or a = 3")
        res = cur2.fetchall()
        assert 2 == len(res)

    def test_conn_commit(self):

        conn = vertica_python.connect(conn_info)
        cur = conn.cursor()
        init_table(cur)
    
        cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (5, 'cc') """)
        conn.commit()
        cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 5")
        res = cur.fetchall()
        assert 1 == len(res)


    def test_update(self):

        conn = vertica_python.connect(conn_info)
        cur = conn.cursor()
        init_table(cur)
    
        cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (5, 'cc') """)
        conn.commit()
    
        cur.execute(""" UPDATE vertica_python_unit_test SET b = 'ff' WHERE a = 5 """)
        conn.commit()
    
        cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 5")
        res = cur.fetchall()
        assert 1 == len(res)
        assert 5 == res[0][0]
        assert 'ff' == res[0][1]

    def test_copy(self):

        conn = vertica_python.connect(conn_info)
        cur = conn.cursor()
        init_table(cur)
    
        conn2 = vertica_python.connect(conn_info)
        cur2 = conn.cursor()
    
        cur.copy(""" COPY vertica_python_unit_test (a, b) from stdin DELIMITER ',' """,  "1,foo\n2,bar")
        # no commit necessary for copy
    
        # verify this cursor can see copy data
        cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 1")
        res = cur.fetchall()
        assert 1 == len(res)
        assert 1 == res[0][0]
        assert 'foo' == res[0][1]
    
        # verify other cursor can see copy data
        cur2.execute("SELECT a, b from vertica_python_unit_test WHERE a = 2")
        res = cur2.fetchall()
        assert 1 == len(res)
        assert 2 == res[0][0]
        assert 'bar' == res[0][1]

    def test_with_conn(self):

        with vertica_python.connect(conn_info) as conn:
            cur = conn.cursor()
            init_table(cur)
        
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (1, 'aa'); commit; """)
            cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 1")
            res = cur.fetchall()
            assert 1 == len(res)

    def test_iterator(self):

        with vertica_python.connect(conn_info) as conn:
            cur = conn.cursor()
            init_table(cur)
        
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (1, 'aa') """)
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (2, 'bb') """)
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (3, 'cc') """)
            conn.commit()
        
            cur.execute("SELECT a, b from vertica_python_unit_test ORDER BY a ASC")
        
            i = 0;
            for row in cur.iterate():
                if i == 0:
                    assert 1 == row[0]
                    assert 'aa' == row[1]
                if i == 1:
                    assert 2 == row[0]
                    assert 'bb' == row[1]
                if i == 2:
                    assert 3 == row[0]
                    assert 'cc' == row[1]
                i = i + 1


    def test_mid_iterator_execution(self):

        with vertica_python.connect(conn_info) as conn:
            cur = conn.cursor()
            init_table(cur)
        
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (1, 'aa') """)
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (2, 'bb') """)
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (3, 'cc') """)
            conn.commit()
        
            cur.execute("SELECT a, b from vertica_python_unit_test ORDER BY a ASC")
        
            # don't finish iterating
            for row in cur.iterate():
                break;

            # make new query and verify result
            cur.execute(""" SELECT COUNT(*) FROM vertica_python_unit_test """)
            res = cur.fetchall()
            assert 1 == len(res)
            assert 3 == res[0][0]


    def test_query_errors(self):
        conn = vertica_python.connect(conn_info)
        cur = conn.cursor()
        init_table(cur)
        
        failed = False;
        # create table syntax error
        try:
            failed = False;
            cur.execute("""CREATE TABLE vertica_python_unit_test_fail (
                            a int,
                            b varchar(32),,,
                            ) ;
                        """)
        except errors.VerticaSyntaxError:
            failed = True;
        assert True == failed
    
        # select table not found error
        try:
            failed = False;
            cur.execute(""" INSERT INTO vertica_python_unit_test (a, b) VALUES (1, 'aa') """)
            cur.execute(""" SELECT * from vertica_python_unit_test_fail  """)
            #conn.commit()
        except errors.QueryError:
            failed = True;
        assert True == failed

        # verify cursor still useable after errors
        cur.execute("SELECT a, b from vertica_python_unit_test WHERE a = 1")
        res = cur.fetchall()
        assert 1 == len(res)
        assert 1 == res[0][0]
        assert 'aa' == res[0][1]
