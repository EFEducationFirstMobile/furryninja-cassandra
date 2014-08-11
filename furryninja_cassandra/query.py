from string import lower
from cassandra.query import ValueSequence
from furryninja import Query, FilterInNode, Key

__author__ = 'broken'


class CassandraQuery(object):
    def __init__(self, query):
        assert isinstance(query, Query), 'query must be a instance of Query, got %r' % query
        self.query = query

    @staticmethod
    def _where_clause(query_filters):
        def stringify_keys(value):
            if isinstance(value, Key):
                return value.urlsafe()
            return value

        query_string = ''
        condition_values = {}
        if query_filters:
            query_string += ' WHERE'
            for index in xrange(len(query_filters)):
                if index > 0:
                    query_string += ' AND'
                qry_filter = query_filters[index]

                query_string += ' %s %s' % (qry_filter.name, qry_filter.opsymbol)
                query_string += ' %(' + qry_filter.name + ')s'

                if isinstance(query_filters[index], FilterInNode):
                    condition_values.update({
                        qry_filter.name: ValueSequence([stringify_keys(qry.value) for qry in qry_filter.value])
                    })
                else:
                    condition_values.update({
                        qry_filter.name: stringify_keys(qry_filter.value)
                    })

        return query_string, condition_values

    def _limit(self):
        if self.query.limit():
            return ' LIMIT %i' % self.query.limit()
        return ''

    def _offset(self):
        if self.query.offset():
            return ' OFFSET %i' % self.query.offset()
        return ''

    def select(self, fields=None):
        query_fields = ', '.join(fields) if fields else '*'
        query_string = 'SELECT %s FROM %s' % (query_fields, lower(self.query.table))

        where_string, condition_values = self._where_clause(self.query.filters())
        query_string += where_string

        query_string += self._limit()
        query_string += self._offset()

        return query_string, condition_values

    def delete(self):
        assert self.query.filters(), 'A delete statement must have filters'
        query_string = 'DELETE FROM %s' % lower(self.query.table)

        where_string, condition_values = self._where_clause(self.query.filters())
        query_string += where_string

        return query_string, condition_values

    @classmethod
    def insert(cls, table, data):
        query_string = 'INSERT INTO %s (%s) VALUES (%s)' % (lower(table), ', '.join(data.keys()), ', '.join(['%(' + name + ')s' for name in data.keys()]))
        condition_values = data
        return query_string, condition_values

    @classmethod
    def update(cls, table, data, where):
        query_string = 'UPDATE %s SET %s' % (lower(table), ', '.join([name +' = %(' + name + ')s' for name in data.keys()]))
        if not isinstance(where, list):
            where = [where]

        where_string, condition_values = cls._where_clause(where)
        query_string += where_string

        condition_values.update(data)
        return query_string, condition_values
