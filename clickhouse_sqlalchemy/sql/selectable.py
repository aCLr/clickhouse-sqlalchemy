from sqlalchemy.sql.selectable import (
    Select as StandardSelect,
    Join as StandardJoin,
)

from ..ext.clauses import sample_clause


__all__ = ('Select', 'select')


class Join(StandardJoin):

    def __init__(self, left, right, onclause=None, type=None, strictness=None, distribution=None):
        if not type:
            raise ValueError('JOIN type must be specified, '
                             'expected one of: '
                             'INNER, RIGHT OUTER, LEFT OUTER, FULL OUTER, CROSS')
        super().__init__(left, right, onclause)
        self.strictness = None
        if strictness:
            self.strictness = strictness
        self.distribution = distribution
        self.type = type


class Select(StandardSelect):
    _with_totals = False
    _sample_clause = None
    _array_join = None

    def with_totals(self):
        self._with_totals = True
        return self

    def sample(self, sample):
        self._sample_clause = sample_clause(sample)
        return self

    def array_join(self, *columns):
        self._array_join = columns
        return self

    def join(self, right, onclause=None, isouter=False, full=False, strictness='ALL', distribution=None):
        return Join(self, right,
                    onclause=onclause, isouter=isouter, full=full,
                    strictness=strictness, distribution=distribution)


select = Select
join = Join
