from sqlalchemy import Column, Table as StandardTable, Integer

from clickhouse_sqlalchemy import types, select, Table
from tests.testcase import BaseTestCase


class SelectTestCase(BaseTestCase):
    def create_table(self, table_name=None, columns=None):
        metadata = self.metadata()
        columns = columns or ['x']
        return Table(
            table_name or 't1', metadata,
            *[
                Column(c, types.Int32, primary_key=True)
                for c in columns
            ]
        )

    def test_group_by_with_totals(self):
        table = self.create_table()

        query = select([table.c.x]).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x FROM t1 GROUP BY x'
        )

        query = select([table.c.x]).group_by(table.c.x).with_totals()
        self.assertEqual(
            self.compile(query),
            'SELECT x FROM t1 GROUP BY x WITH TOTALS'
        )

    def test_sample(self):
        table = self.create_table()

        query = select([table.c.x]).sample(0.1).group_by(table.c.x)
        self.assertEqual(
            self.compile(query),
            'SELECT x FROM t1 SAMPLE %(param_1)s GROUP BY x'
        )

        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT x FROM t1 SAMPLE 0.1 GROUP BY x'
        )

    def test_select_from_table(self):
        table = self.create_table()
        query = table.select().sample(0.1)
        self.assertEqual(
            self.compile(query),
            'SELECT x FROM t1 SAMPLE %(param_1)s'
        )

    def test_join(self):
        table_1 = self.create_table('table_1', 'x')
        table_2 = self.create_table('table_2', 'y')

        def make_statement(type=None, strictness=None, distribution=None):
            join = table_1.join(
                table_2,
                table_2.c.y == table_1.c.x,
                type=type,
                strictness=strictness,
                distribution=distribution
            )
            return select([table_1.c.x]).select_from(join)

        self.assertRaises(
            ValueError,
            lambda: make_statement(type=None)
        )

        self.assertEqual(
            self.compile(make_statement(type='INNER')),
            'SELECT x FROM table_1 INNER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='INNER', strictness='all')),
            'SELECT x FROM table_1 ALL INNER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='INNER', strictness='any')),
            'SELECT x FROM table_1 ANY INNER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='INNER', distribution='global')),
            'SELECT x FROM table_1 GLOBAL INNER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='INNER', distribution='global', strictness='any')),
            'SELECT x FROM table_1 GLOBAL ANY INNER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='INNER', distribution='global', strictness='all')),
            'SELECT x FROM table_1 GLOBAL ALL INNER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='LEFT OUTER', distribution='global', strictness='all')),
            'SELECT x FROM table_1 GLOBAL ALL LEFT OUTER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='RIGHT OUTER', distribution='global', strictness='all')),
            'SELECT x FROM table_1 GLOBAL ALL RIGHT OUTER JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='CROSS', distribution='global', strictness='all')),
            'SELECT x FROM table_1 GLOBAL ALL CROSS JOIN table_2 ON y = x'
        )

        self.assertEqual(
            self.compile(make_statement(type='FULL OUTER')),
            'SELECT x FROM table_1 FULL OUTER JOIN table_2 ON y = x'
        )
