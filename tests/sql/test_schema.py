from clickhouse_sqlalchemy import types, Table, engines
from sqlalchemy import MetaData, Column, Table as StandardTable
from tests.session import native_session
from tests.testcase import BaseTestCase


class SchemaTestCase(BaseTestCase):
    def test_reflect(self):
        unbound_metadata = MetaData(bind=native_session.bind)
        table = Table(
            'test_reflect',
            unbound_metadata,
            Column('x', types.Int32),
            engines.Log()
        )
        table.drop(native_session.bind, if_exists=True)
        table.create(native_session.bind)

        std_metadata = self.metadata()
        self.assertFalse(std_metadata.tables)
        std_metadata.reflect(only=[table.name])
        self.assertTrue(isinstance(std_metadata.tables.get(table.name), Table))
        self.assertTrue(isinstance(std_metadata.tables[table.name], StandardTable))
