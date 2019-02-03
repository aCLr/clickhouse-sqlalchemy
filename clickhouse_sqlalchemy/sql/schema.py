from sqlalchemy import Table as TableBase
from sqlalchemy.sql.base import _bind_or_error

from clickhouse_sqlalchemy.sql.selectable import Join
from . import ddl


class Table(TableBase):
    def drop(self, bind=None, checkfirst=False, if_exists=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper,
                          self,
                          checkfirst=checkfirst, if_exists=if_exists)

    def join(self, right, onclause=None, type=None, full=False, strictness='ALL', distribution=None):
        return Join(self, right,
                    onclause=onclause, type=type,
                    strictness=strictness, distribution=distribution)
