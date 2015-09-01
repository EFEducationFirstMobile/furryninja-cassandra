# -*- coding: utf-8 -*-
from collections import OrderedDict
from itertools import ifilter
import datetime
import pytz
import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import HostDistance
from cassandra.query import ordered_dict_factory, BatchStatement, SimpleStatement
from furryninja.model import AttributesProperty, DateTimeProperty

from furryninja.repository import Repository
from furryninja import Settings, KeyProperty, Key, Model, StringProperty, QueryNotFoundException
from .model import CassandraModelMixin
from .query import CassandraQuery
from .exceptions import PrimaryKeyException, ModelValidationException, LightweightTransactionException

logger = logging.getLogger('cassandra.repo')



class CassandraRepository(Repository):

    def __init__(self, connection_class=Cluster, construct_primary_key=None, edge_model=None):
        super(CassandraRepository, self).__init__()

        self.settings = dict(host='localhost', port=9042, protocol_version=2)
        self.settings.update(Settings.get('db'))

        assert self.settings.get('name', None), 'Missing required setting db.name'

        if not isinstance(self.settings.get('port'), int):
            self.settings['port'] = int(self.settings.get('port'))
        if not isinstance(self.settings.get('protocol_version'), int):
            self.settings['protocol_version'] = int(self.settings.get('protocol_version'))

        cluster = connection_class(
            contact_points=self.settings['host'],
            port=self.settings['port'],
            protocol_version=self.settings['protocol_version']
        )
        cluster.set_core_connections_per_host(HostDistance.LOCAL, 10)
        self.session = cluster.connect(keyspace=self.settings['name'])
        self.session.row_factory = ordered_dict_factory

        if construct_primary_key:
            self.construct_primary_key = construct_primary_key

    def _get_table_metadata(self, table_name):
        return self.session.cluster.metadata.keyspaces[Settings.get('db.name')].tables[table_name]

    def _get_primary_key_fields(self, model):
        metadata = self._get_table_metadata(model.table())
        return [field.name for field in metadata.primary_key]

    def _execute(self, cql_qry, serial_consistency_level=None):
        assert isinstance(cql_qry, CassandraQuery), 'cql_qry should be of type CassandraQuery'

        if self.settings.get('serial_consistency_level', None) and not serial_consistency_level:
            serial_consistency_level = int(self.settings.get('serial_consistency_level'))

        stmt = SimpleStatement(cql_qry.statement, serial_consistency_level=serial_consistency_level)
        result = self.session.execute(stmt, parameters=cql_qry.condition_values)

        # Cassandra is amazing. But someone did something stupid here.
        if isinstance(result, list) and len(result) > 0 and isinstance(result[0], OrderedDict):
            if result[0].get('[applied]', None) and result[0]['[applied]'] is False:
                raise LightweightTransactionException('Failed to apply transaction')

        return result
    execute = _execute

    def _execute_batch(self, batch):
        result = self.session.execute(batch)

        # Cassandra is amazing. But someone did something stupid here.
        if isinstance(result, list) and len(result) > 0 and isinstance(result[0], OrderedDict):
            if result[0].get('[applied]', None) is not None and result[0]['[applied]'] is False:
                raise LightweightTransactionException('Failed to apply transaction')

        return result
    execute_batch = _execute_batch

    @staticmethod
    def _construct_primary_key(model, metadata):
        fields = {}
        for key_part in metadata.primary_key:
            if not hasattr(model, key_part.name):
                raise PrimaryKeyException('Missing mandatory PRIMARY KEY part %r' % key_part.name)

            value = getattr(model, key_part.name)
            if isinstance(value, Key):
                value = value.urlsafe()
            elif value is not None:
                value = CassandraRepository._cassandra_type_string_to_type(key_part.typestring)(value)
            fields[key_part.name] = value

        return fields
    construct_primary_key = _construct_primary_key

    @staticmethod
    def __validate_model(model):
        if not isinstance(model, CassandraModelMixin):
            raise ModelValidationException('Expected model to be an instance of CassandraModelMixin, got %r' % model)
    validate_model = __validate_model

    @staticmethod
    def _cassandra_type_string_to_type(type_string):
        type_map = {
            'text': str,
            'int': int
        }

        assert type_string in type_map, 'Unknown type_string "%s"' % type_string

        return type_map[type_string]

    @staticmethod
    def denormalize(model):
        if hasattr(model, '_storage_type_to_db') and callable(getattr(model, '_storage_type_to_db')):
            return model._storage_type_to_db(serialize_fn=lambda x: x)

        return model.entity_to_db()

    def fetch(self, query, fields=None):
        result = []
        cql_qry = CassandraQuery(query).select()
        rows = self._execute(cql_qry)

        for row in rows:
            model_cls = Model._lookup_model(Key.from_string(row['key']).kind)
            model_data = model_cls._db_to_storage_type(row)
            model = model_cls(**model_data)
            self.resolve_referenced_keys(model, fields=fields)
            result.append(model)
        return result

    def get(self, model, fields=None):
        self.__validate_model(model)

        query = model.query(*[getattr(model.__class__, field) == getattr(model, field) for field in self._get_primary_key_fields(model)]).limit(1)
        cql_qry = CassandraQuery(query).select()
        rows = self._execute(cql_qry)

        if not rows:
            raise QueryNotFoundException

        row = rows[0]
        model_data = model.__class__._db_to_storage_type(row)
        model.populate(**model_data)
        self.resolve_referenced_keys(model, fields=fields)
        return model

    def delete(self, model):
        self.__validate_model(model)

        query = model.query(*[getattr(model.__class__, field) == getattr(model, field) for field in self._get_primary_key_fields(model)]).limit(1)
        cql_qry = CassandraQuery(query).delete()
        self._execute(cql_qry)

    def __insert(self, models, if_not_exists=None):
        assert models, 'You can insert nothing, what good would that do?'

        serial_consistency_level = None
        if if_not_exists:
            serial_consistency_level = ConsistencyLevel.SERIAL

        batch = BatchStatement(serial_consistency_level=serial_consistency_level)

        for model in models:
            self.__validate_model(model)

            model._pre_put_hook()
            metadata = self._get_table_metadata(model.table())

            fields = self.denormalize(model)
            fields.update(self.construct_primary_key(model, metadata))

            cql_qry = CassandraQuery(model.query()).insert(fields)
            if if_not_exists:
                cql_qry.if_not_exists()
            batch.add(cql_qry.statement, parameters=cql_qry.condition_values)
        self._execute_batch(batch)

        for model in models:
            model._post_put_hook()

        if len(models) == 1:
            return models[0]
        return models

    def insert(self, model, if_not_exists=None):
        return self.__insert([model], if_not_exists=if_not_exists)

    def insert_multi(self, models, if_not_exists=None):
        return self.__insert(models, if_not_exists=if_not_exists)

    def update(self, model, update_if=None):
        self.__validate_model(model)

        model._pre_put_hook()

        fields = self.denormalize(model)
        where = []
        serial_consistency_level = None

        for field in self._get_primary_key_fields(model):
            if field in fields:
                del fields[field]

            where.append(getattr(model.__class__, field) == getattr(model, field))
        assert fields.keys(), 'Model has no properties.'

        cql_qry = CassandraQuery(model.query(*where)).update(fields)
        if update_if:
            assert isinstance(update_if, tuple) and len(update_if) == 2, 'update_if should be a tuple (field, value) of length 2'
            cql_qry.update_if(update_if[0], update_if[1])
            serial_consistency_level = ConsistencyLevel.SERIAL

        self._execute(cql_qry, serial_consistency_level=serial_consistency_level)

        model._post_put_hook()

        return model
