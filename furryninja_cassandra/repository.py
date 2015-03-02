# -*- coding: utf-8 -*-
from itertools import ifilter
import datetime
from cassandra import ConsistencyLevel

import pytz
import logging

from cassandra.cluster import Cluster
from cassandra.policies import HostDistance
from cassandra.query import ordered_dict_factory, BatchStatement, SimpleStatement
from furryninja.model import AttributesProperty, DateTimeProperty

from furryninja.repository import Repository
from furryninja import Settings, KeyProperty, Key, Model, StringProperty, QueryNotFoundException
from .model import CassandraModelMixin
from .query import CassandraQuery
from .exceptions import PrimaryKeyException, ModelValidationException

logger = logging.getLogger('cassandra.repo')
LIBEV = False

try:
    from cassandra.io.libevreactor import LibevConnection
    LIBEV = True
except ImportError:
    logger.warn('Not using libev, this is bad. Install it!!')


class Edge(Model, CassandraModelMixin):
    _storage_type = ('simple',)

    label = StringProperty()
    indoc = KeyProperty()
    outdoc = KeyProperty()
    create_date = DateTimeProperty(auto_now_add=True)
    last_update = DateTimeProperty(auto_now=True)


class CassandraRepository(Repository):
    def __init__(self, connection_class=Cluster):
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
        if LIBEV:
            cluster.connection_class = LibevConnection

        self.session = cluster.connect(keyspace=self.settings['name'])
        self.session.row_factory = ordered_dict_factory

    def __get_table_metadata(self, table_name):
        return self.session.cluster.metadata.keyspaces[Settings.get('db.name')].tables[table_name]

    def __get_primary_key_fields(self, model):
        metadata = self.__get_table_metadata(model.table())
        return [field.name for field in metadata.primary_key]

    def __construct_primary_key(self, model):
        metadata = self.__get_table_metadata(model.table())
        fields = {}
        for key_part in metadata.primary_key:
            if not hasattr(model, key_part.name):
                raise PrimaryKeyException('Missing mandatory PRIMARY KEY part %r' % key_part.name)

            fields[key_part.name] = '%s' % getattr(model, key_part.name)

        return fields

    def __execute(self, cql_qry):
        assert isinstance(cql_qry, CassandraQuery), 'cql_qry should be of type CassandraQuery'
        serial_consistency_level = None
        if self.settings.get('serial_consistency_level', None):
            serial_consistency_level = int(self.settings.get('serial_consistency_level'))
        # For conditional DELETE, INSERT or UPDATES use lightweight transactions with ConsistencyLevel.SERIAL
        if 'IF' in cql_qry.statement:
            serial_consistency_level = ConsistencyLevel.SERIAL

        stmt = SimpleStatement(cql_qry.statement, serial_consistency_level=serial_consistency_level)
        return self.session.execute(stmt, parameters=cql_qry.condition_values)

    def __execute_batch(self, batch):
        return self.session.execute(batch)

    @staticmethod
    def __validate_model(model):
        if not isinstance(model, CassandraModelMixin):
            raise ModelValidationException('Expected model to be an instance of CassandraModelMixin, got %r' % model)

    @staticmethod
    def denormalize(model):
        def get_value(attr):
            attr_value = attr or None
            if isinstance(attr, (KeyProperty, Key)):
                attr_value = attr.urlsafe()

            if isinstance(attr, Model):
                attr_value = attr.key.urlsafe()

            if isinstance(attr, list):
                attr_value = [get_value(item) for item in attr]

            if isinstance(attr, dict):
                attr_value = attr

            if isinstance(attr, datetime.datetime):
                attr_value = attr.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S.%f%z')

            return attr_value

        def serialize_keys(serialized_node):
            props = ifilter(lambda x: isinstance(x[1], (Key, KeyProperty, Model, list, dict, datetime.datetime)), serialized_node.items())
            for name, prop in props:
                if isinstance(prop, dict):
                    value = serialize_keys(prop)
                else:
                    value = get_value(prop)
                serialized_node[name] = value
            return serialized_node

        if hasattr(model, '_storage_type_to_db') and callable(getattr(model, '_storage_type_to_db')):
            return model._storage_type_to_db(serialize_fn=serialize_keys)

        return serialize_keys(model.entity_to_db())

    def set_edges_for_model(self, model, new_edges=None, existing_edges=None):
        assert new_edges
        if not existing_edges:
            existing_edges = []

        model_edges_combinations = dict([('%s-%s' % (e.label, e.outdoc.urlsafe()), e) for e in existing_edges])

        for edge in new_edges:
            combinations_id = '%s-%s' % (edge.label, edge.outdoc)
            if not combinations_id in model_edges_combinations:
                edge.indoc = model.key
                self.insert_edge(edge)
            else:
                del model_edges_combinations[combinations_id]
        self.delete_edge(model_edges_combinations.values())

    @staticmethod
    def find_edges(model):
        def edge(label, outdoc):
            return Edge(**{
                'label': label,
                'outdoc': outdoc
            })

        def find(obj, node, path, edges):
            for name in node:
                current_path = path + [name]
                attr = obj._attributes_map.get(name) if isinstance(obj, AttributesProperty) else getattr(obj, name)
                if isinstance(attr, AttributesProperty):
                    value = attr._get_value(model)
                    if isinstance(value, list):
                        for item in value:
                            find(attr, item.keys(), current_path, edges)
                    else:
                        find(attr, value._attributes_map.keys(), current_path, edges)
                if isinstance(attr, KeyProperty):
                    if attr._repeated:
                        for value in attr._get_value(model):
                            if value is not None:
                                edges.append(edge('.'.join(current_path), value.key if isinstance(value, Model) else value))
                    else:
                        value = attr._get_value(model)
                        if value is not None:
                            edges.append(edge('.'.join(current_path), value.key if isinstance(value, Model) else value))

        found_edges = []
        find(model.__class__, ifilter(lambda x: x != model.__class__._key_property_name, set(dir(model.__class__))), [], found_edges)
        sorted(found_edges)
        return dict([('%s-%s' % (e.label, e.outdoc.urlsafe()), e) for e in found_edges]).values()

    def fetch(self, query, fields=None):
        result = []
        cql_qry = CassandraQuery(query).select()
        rows = self.__execute(cql_qry)

        for row in rows:
            model_cls = Model._lookup_model(Key.from_string(row['key']).kind)
            model_data = model_cls._db_to_storage_type(row)
            model = model_cls(**model_data)
            self.resolve_referenced_keys(model, fields=fields)
            result.append(model)
        return result

    def get(self, model, fields=None):
        self.__validate_model(model)

        query = model.query(*[getattr(model.__class__, field) == getattr(model, field) for field in self.__get_primary_key_fields(model)]).limit(1)
        cql_qry = CassandraQuery(query).select()
        rows = self.__execute(cql_qry)

        if not rows:
            raise QueryNotFoundException

        row = rows[0]
        model_data = model.__class__._db_to_storage_type(row)
        model.populate(**model_data)
        self.resolve_referenced_keys(model, fields=fields)
        return model

    def delete(self, model):
        self.__validate_model(model)

        edge_query = Edge.query(Edge.indoc == model.key)
        cql_qry = CassandraQuery(edge_query).select()

        existing_edges = self.__execute(cql_qry)
        self.delete_edge(existing_edges)

        query = model.query(*[getattr(model.__class__, field) == getattr(model, field) for field in self.__get_primary_key_fields(model)]).limit(1)
        cql_qry = CassandraQuery(query).delete()
        self.__execute(cql_qry)

    def delete_edge(self, models):
        if models and self.settings['protocol_version'] >= 2:
            batch = BatchStatement()
            for edge in models:
                if isinstance(edge, dict):
                    edge = Edge(**edge)
                cql_qry = CassandraQuery(Edge.query(Edge.indoc == edge.indoc, Edge.outdoc == edge.outdoc, Edge.label == edge.label)).delete()
                batch.add(cql_qry.statement, parameters=cql_qry.condition_values)

            self.__execute_batch(batch)
        elif models:
            for edge in models:
                if isinstance(edge, dict):
                    edge = Edge(**edge)
                cql_qry = CassandraQuery(Edge.query(Edge.indoc == edge.indoc, Edge.outdoc == edge.outdoc, Edge.label == edge.label)).delete()
                self.__execute(cql_qry)

    def insert_edge(self, model):
        cql_qry = CassandraQuery(Edge.query()).insert({
            'key': model.key.urlsafe(),
            'label': model.label,
            'indoc': model.indoc.urlsafe(),
            'outdoc': model.outdoc.urlsafe()
        })

        self.__execute(cql_qry)

    def __insert(self, models):
        assert models, 'You can insert nothing, what good would that do?'
        batch = BatchStatement()
        for model in models:
            self.__validate_model(model)

            model._pre_put_hook()

            fields = self.denormalize(model)
            fields.update(self.__construct_primary_key(model))

            cql_qry = CassandraQuery(model.query()).insert(fields)
            batch.add(cql_qry.statement, parameters=cql_qry.condition_values)
        self.__execute_batch(batch)

        for model in models:
            model._post_put_hook()

            edges = self.find_edges(model)
            if edges:
                self.set_edges_for_model(model, edges)
        if len(models) == 1:
            return models[0]
        return models

    def insert(self, model):
        return self.__insert([model])

    def insert_multi(self, models):
        return self.__insert(models)

    def update(self, model, update_if=None):
        self.__validate_model(model)

        model._pre_put_hook()

        fields = self.denormalize(model)
        where = []
        for field in self.__get_primary_key_fields(model):
            if field in fields:
                del fields[field]

            where.append(getattr(model.__class__, field) == getattr(model, field))
        assert fields.keys(), 'Model has no properties.'

        cql_qry = CassandraQuery(model.query(*where)).update(fields)
        if update_if:
            assert isinstance(update_if, tuple) and len(update_if) == 2, 'update_if should be a tuple (field, value) of length 2'
            cql_qry.update_if(update_if[0], update_if[1])

        self.__execute(cql_qry)

        model._post_put_hook()

        existing_edges = self.fetch(Edge.query(Edge.indoc == model.key))
        edges = self.find_edges(model)
        if edges:
            self.set_edges_for_model(model, edges, existing_edges)
        return model