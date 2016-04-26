from .test_commons import conn_info, VerticaTestCase
from .. import connect
from .. import errors


class ErrorTestCase(VerticaTestCase):

    def test_missing_schema(self):

        query = "SELECT 1 FROM missing_schema.table"

        with connect(**conn_info) as conn:
            cur = conn.cursor()

            failed = False

            try:
                cur.execute(query)
            except errors.MissingSchema:
                failed = True

            assert failed is True

    def test_missing_relation(self):

        query = "SELECT 1 FROM missing_table"

        with connect(**conn_info) as conn:
            cur = conn.cursor()

            failed = False

            try:
                cur.execute(query)
            except errors.MissingRelation:
                failed = True

            assert failed is True

    def test_duplicate_object(self):

        create = "CREATE TABLE test_table (a BOOLEAN)"
        drop = "DROP TABLE test_table"

        with connect(**conn_info) as conn:
            cur = conn.cursor()

            failed = False

            cur.execute(create)

            try:
                cur.execute(create)
            except errors.DuplicateObject:
                failed = True
            finally:
                try:
                    cur.execute(drop)
                except errors.MissingRelation:
                    pass

            assert failed is True
