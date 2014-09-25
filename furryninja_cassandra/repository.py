# -*- coding: utf-8 -*-
from itertools import ifilter
from string import lower
import datetime
import pytz

import simplejson as json

from cassandra.cluster import Cluster
from cassandra.policies import HostDistance
from cassandra.query import dict_factory, BatchStatement
from furryninja.model import AttributesProperty, DateTimeProperty

from furryninja.repository import Repository
from furryninja import Settings, KeyProperty, Key, Model, StringProperty, QueryNotFoundException
from .query import CassandraQuery
import logging

logger = logging.getLogger('cassandra.repo')


class Edge(Model):
    label = StringProperty()
    indoc = KeyProperty()
    outdoc = KeyProperty()
    create_date = DateTimeProperty(auto_now_add=True)
    last_update = DateTimeProperty(auto_now=True)


class CassandraRepository(Repository):
    def __init__(self, connection_class=Cluster):
        cluster = connection_class(Settings.get('db.host'))
        cluster.set_core_connections_per_host(HostDistance.LOCAL, 10)
        self.session = cluster.connect(keyspace=Settings.get('db.name'))
        self.session.row_factory = dict_factory

    def denormalize(self, model):
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

        serialized = serialize_keys(model.entity_to_db())
        return serialized

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
        cql_statement, condition_values = CassandraQuery(query).select()
        rows = self.session.execute(cql_statement, parameters=condition_values)

        for row in rows:
            model_data = json.loads(row['blob']) if row.get('blob', None) else row
            model_cls = Model._lookup_model(Key.from_string(row['key']).kind)
            model = model_cls(**model_data)
            self.resolve_referenced_keys(model, fields=fields)
            result.append(model)
        return result

    def get(self, model, fields=None):
        query = model.query(model.__class__.key == model.key).limit(1)
        cql_statement, condition_values = CassandraQuery(query).select()
        rows = self.session.execute(cql_statement, parameters=condition_values)

        if not rows:
            raise QueryNotFoundException

        model_data = json.loads(rows[0]['blob']) if rows[0].get('blob', None) else rows[0]
        model.populate(**model_data)
        self.resolve_referenced_keys(model, fields=fields)
        return model

    def delete(self, model):
        edge_query = Edge.query(Edge.indoc == model.key)
        edge_cql_statement, condition_values = CassandraQuery(edge_query).select()

        existing_edges = self.session.execute(edge_cql_statement, parameters=condition_values)
        self.delete_edge(existing_edges)

        query = model.query(model.__class__.key == model.key).limit(1000)
        cql_statement, condition_values = CassandraQuery(query).delete()
        self.session.execute(cql_statement, parameters=condition_values)

    def delete_edge(self, models):
        if models:
            batch = BatchStatement()
            for edge in models:
                if isinstance(edge, dict):
                    edge = Edge(**edge)
                cql_statement, condition_values = CassandraQuery(Edge.query(Edge.indoc == edge.indoc, Edge.outdoc == edge.outdoc, Edge.label == edge.label)).delete()
                batch.add(cql_statement, parameters=condition_values)

            self.session.execute(batch)

    def insert_edge(self, model):
        cql_statement, condition_values = CassandraQuery.insert(Edge.table(), {
            'key': model.key.urlsafe(),
            'label': model.label,
            'indoc': model.indoc.urlsafe(),
            'outdoc': model.outdoc.urlsafe()
        })

        self.session.execute(cql_statement, parameters=condition_values)

    def insert(self, model):
        assert isinstance(model, Model), 'Expected a Model instance, got %r' % model
        model._pre_put_hook()

        model_as_dict = self.denormalize(model)
        blob = json.dumps(model_as_dict)
        fields = {
            'key': model.key.urlsafe(),
            'blob': blob
        }

        if hasattr(model, 'revision'):
            fields.update({'revision': model.revision})

        cql_statement, condition_values = CassandraQuery.insert(model.table(), fields)
        self.session.execute(cql_statement, parameters=condition_values)

        model._post_put_hook()

        edges = self.find_edges(model)
        if edges:
            self.set_edges_for_model(model, edges)
        return model

    def update(self, model):
        assert isinstance(model, Model), 'Expected a Model instance, got %r' % model
        model._pre_put_hook()

        model_as_dict = self.denormalize(model)
        blob = json.dumps(model_as_dict)
        fields = {
            'blob': blob
        }

        where = [model.__class__.key == model.key]
        if hasattr(model, 'revision'):
            where.append(model.__class__.revision == model.revision)

        cql_statement, condition_values = CassandraQuery.update(model.table(), fields, where)
        self.session.execute(cql_statement, parameters=condition_values)

        model._post_put_hook()

        existing_edges = self.fetch(Edge.query(Edge.indoc == model.key))
        edges = self.find_edges(model)
        if edges:
            self.set_edges_for_model(model, edges, existing_edges)
        return model