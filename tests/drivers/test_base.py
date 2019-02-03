from sqlalchemy import Column

from clickhouse_sqlalchemy import (
    Table,
    types,
    engines,
)
from tests.session import native_session
from tests.testcase import BaseTestCase


class ClickHouseDialectTestCase(BaseTestCase):
    def create_table(self, engine):
        table = Table(
            'test_reflect',
            self.metadata(),
            Column('x', types.Int32),
            Column('date', types.Date),
            engine
        )
        table.drop(native_session.bind, if_exists=True)
        table.create(native_session.bind)
        return table

    def test_get_engine(self):
        # table = self.create_table(engines.Log())
        # engine = native_session.bind.dialect.get_engine(native_session.bind, table.name)
        # self.assertEqual(engine, table.dialect_options['clickhouse']['engine'])

        table = self.create_table(engines.MergeTree('date', ('x')))
        engine = native_session.bind.dialect.get_engine(native_session.bind, table.name)
        self.assertEqual(engine, table.dialect_options['clickhouse']['engine'])
