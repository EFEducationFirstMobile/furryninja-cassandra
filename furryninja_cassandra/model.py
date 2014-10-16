import datetime
import pytz
import simplejson as json
from simplejson import JSONEncoder
from furryninja import key_ref, Model, Key

__author__ = 'broken'


class ModelJsonEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Key):
            return o.urlsafe()
        elif isinstance(o, Model):
            return o.entity_values(unset=True)
        elif isinstance(o, datetime.datetime):
            return o.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        else:
            return super(ModelJsonEncoder, self).default(o)

json_encoder = ModelJsonEncoder()


class CassandraModelMixin(object):
    _storage_type = ('simple', )

    def _storage_type_to_db(self):
        assert self._storage_type[0] in ['simple', 'json'], '_storage_type must be an iterable with a first element of "simple" or "json"'

        if self._storage_type[0] == 'json':
            assert self._storage_type[1], '_storage_type second element must a string'

            model = self.entity_to_db()
            return {
                self._storage_type[1]: json_encoder.encode(model)
            }

        return self.entity_to_db()

    @classmethod
    def _db_to_storage_type(cls, row):
        assert cls._storage_type[0] in ['simple', 'json'], '_storage_type must be an iterable with a first element of "simple" or "json"'

        if cls._storage_type[0] == 'json':
            assert cls._storage_type[1], '_storage_type second element must a string'

            if not row.get(cls._storage_type[1], None):
                raise KeyError('Argument "row" is missing required key "%s"' % cls._storage_type[1])

            return json.loads(row[cls._storage_type[1]])
        return row