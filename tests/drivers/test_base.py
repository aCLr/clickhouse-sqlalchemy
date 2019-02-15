from sqlalchemy import (
    Column,
    func,
)

from clickhouse_sqlalchemy import (
    Table,
    types,
    engines,
)
from tests.session import native_session
from tests.testcase import BaseTestCase


class ClickHouseDialectTestCase(BaseTestCase):

    def test_get_engine(self):
        def assertion(table_name):
            engine = native_session.bind.dialect.get_engine(native_session.bind, table_name)
            self.assertEqual(
                engine,
                table.dialect_options['clickhouse']['engine']
            )

        table = Table(
            'test_reflect',
            self.metadata(),
            Column('x', types.Int32),
            Column('date', types.Date),
            engines.MergeTree(
                partition_by='date',
                order_by=('x',),
                sample='x',
                index_granularity=10
            ))
        table.drop(native_session.bind, if_exists=True)
        table.create(native_session.bind)
        assertion(table.name)
        table.drop(native_session.bind)

        date_col = Column('date', types.Date)
        x_col = Column('x', types.Int32)
        y_col = Column('y', types.Int32)
        table = Table(
            'test_reflect',
            self.metadata(),
            x_col, y_col,
            date_col,
            engines.AggregatingMergeTree(
                partition_by=date_col,
                order_by=(x_col, y_col),
                sample=x_col,
                index_granularity=10
            ))
        table.drop(native_session.bind, if_exists=True)
        table.create(native_session.bind)
        assertion(table.name)
        table.drop(native_session.bind)

        date_col = Column('date', types.Date)
        x_col = Column('x', types.Int8)
        y_col = Column('y', types.Int32)
        table = Table(
            'test_reflect',
            self.metadata(),
            x_col, y_col,
            date_col,
            engines.CollapsingMergeTree(
                'x',
                partition_by=func.toYYYYMM(date_col),
                order_by=('y')
            ))
        table.drop(native_session.bind, if_exists=True)
        table.create(native_session.bind)
        assertion(table.name)
        table.drop(native_session.bind)
