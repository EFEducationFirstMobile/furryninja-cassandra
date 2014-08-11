import json
import unittest
import datetime
from furryninja import Model, StringProperty, FilterNode, key_ref
from furryninja.cassandra.query import CassandraQuery
from furryninja.query import QueryException

__author__ = 'broken'


class ImageAsset(Model):
    title = StringProperty()
    description = StringProperty()


class TestCassandraQuery(unittest.TestCase):
    def test_query_to_select_statement(self):
        qry = ImageAsset.query()
        cql_statement, condition_values = CassandraQuery(qry).select()

        self.assertEqual(cql_statement, 'SELECT * FROM imageasset LIMIT 50')

        qry = ImageAsset.query().limit(42)
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset LIMIT 42')

        qry.offset(10)
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset LIMIT 42 OFFSET 10')

        qry.offset(0).limit(50)
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset LIMIT 50')

    def test_query_to_select_statement_with_fields(self):
        qry = ImageAsset.query()
        cql_statement, condition_values = CassandraQuery(qry).select(fields=['key', 'title'])

        self.assertEqual(cql_statement, 'SELECT key, title FROM imageasset LIMIT 50')

    def test_query_to_select_statement_with_equal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title == 'Lorem Ipsum')
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE title = %(title)s LIMIT 50')

        qry = ImageAsset.query(ImageAsset.title == 'Lorem Ipsum', ImageAsset.description == 'Lorem')
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE title = %(title)s AND description = %(description)s LIMIT 50')

    def test_query_to_select_statement_with_noequal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title != 'Lorem Ipsum')
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE title != %(title)s LIMIT 50')

    def test_query_to_select_statement_with_less_then_comparison(self):
        qry = ImageAsset.query(ImageAsset.title < 'Lorem Ipsum')
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE title < %(title)s LIMIT 50')

    def test_query_to_select_statement_with_less_equal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title <= 'Lorem Ipsum')
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE title <= %(title)s LIMIT 50')

    def test_query_to_select_statement_with_greater_then_comparison(self):
        qry = ImageAsset.query(ImageAsset.title > 'Lorem Ipsum')
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE title > %(title)s LIMIT 50')

    def test_query_to_select_statement_with_greater_equal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title >= 'Lorem Ipsum')
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE title >= %(title)s LIMIT 50')

    def test_query_to_select_statement_with_key_comparison(self):
        class AudioAsset(Model):
            _key_property_name = 'id'

            @key_ref
            def key(self):
                return getattr(self, self._key_property_name)

        image_test_key = ImageAsset().key
        audio_test_key = AudioAsset().key

        qry = ImageAsset.query(ImageAsset.key == image_test_key)
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE key = %(key)s LIMIT 50')

        qry = AudioAsset.query(AudioAsset.key == audio_test_key)
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM audioasset WHERE key = %(key)s LIMIT 50')

    def test_query_with_list(self):
        class AudioAsset(Model):
            titles = StringProperty(repeated=True)

        qry = ImageAsset.query(AudioAsset.titles.IN(['Here', 'There']))
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM imageasset WHERE titles IN %(titles)s LIMIT 50')

        with self.assertRaises(QueryException):
            ImageAsset.query(AudioAsset.titles == ['Here', 'There'])

    def test_query_with_set_table_str(self):
        class VideoAsset(Model):
            table_str = 'asset'

        qry = VideoAsset.query()
        cql_statement, condition_values = CassandraQuery(qry).select()
        self.assertEqual(cql_statement, 'SELECT * FROM asset LIMIT 50')

    def test_insert_query(self):
        asset_data = {
            'title': 'Lorem',
            'description': 'Ipsum'
        }
        asset = ImageAsset()

        cql_statement, condition_values = CassandraQuery.insert(asset.table(), {
            'key': asset.key.urlsafe(),
            'blob': json.dumps(asset_data)
        })

        self.assertEqual(cql_statement, 'INSERT INTO imageasset (blob, key) VALUES (%(blob)s, %(key)s)')

    def test_update_query(self):
        asset_data = {
            'title': 'Lorem',
            'description': 'Ipsum'
        }
        asset = ImageAsset()
        last_update = str(datetime.datetime.now())
        cql_statement, condition_values = CassandraQuery.update(asset.table(), {
            'blob': json.dumps(asset_data),
            'last_update': last_update
        }, ImageAsset.key == asset.key)

        self.assertEqual(cql_statement, 'UPDATE imageasset SET blob = %(blob)s, last_update = %(last_update)s WHERE key = %(key)s')