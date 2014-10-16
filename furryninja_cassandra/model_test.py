import unittest
from furryninja import Model
from furryninja.model import StringProperty
from furryninja_cassandra.model import CassandraModelMixin
from furryninja_cassandra.repository import CassandraRepository

__author__ = 'broken'


class Book(Model, CassandraModelMixin):
    title = StringProperty()


class Asset(Model, CassandraModelMixin):
    _storage_type = ('json', 'blob')
    title = StringProperty()


class TestCassandraModel(unittest.TestCase):
    def test_storage_type_simple_to_db(self):
        book = Book(**{'title': 'A storm of swords'})
        denormalized = CassandraRepository.denormalize(book)

        self.assertDictEqual(denormalized, {
            'key': book.key.urlsafe(),
            'title': 'A storm of swords'
        })

    def test_storage_type_json_to_db(self):
        entity = Asset(**{'title': 'A storm of swords'})
        denormalized = CassandraRepository.denormalize(entity)

        self.assertDictEqual(denormalized, {
            'blob': '{"key": "%s", "title": "A storm of swords"}' % entity.key.urlsafe()
        })

    def test_db_to_storage_type_simple(self):
        entity = Book(**{'title': 'A storm of swords'})
        row = {
            'key': entity.key.urlsafe(),
            'title': 'A storm of swords'
        }

        self.assertDictEqual(entity.__class__._db_to_storage_type(row), entity.entity_to_db())

    def test_db_to_storage_type_json(self):
        entity = Asset(**{'title': 'A storm of swords'})
        row = {
            'blob': '{"key": "%s", "title": "A storm of swords"}' % entity.key.urlsafe()
        }

        self.assertDictEqual(entity.__class__._db_to_storage_type(row), entity.entity_to_db())