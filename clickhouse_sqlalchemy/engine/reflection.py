from sqlalchemy.engine.reflection import Inspector as BaseInspector


class CHInspector(BaseInspector):
    def reflecttable(self, table, include_columns, exclude_columns=(), _extend_on=None):
        super().reflecttable(table, include_columns, exclude_columns, _extend_on)
        self._reflect_engine(table.name, table.schema, table)

    def _reflect_engine(
            self, table_name, schema, table):
        engine = self.dialect.get_engine(self.bind, table_name, schema)
        table.dialect_options['clickhouse']['engine'] = engine
