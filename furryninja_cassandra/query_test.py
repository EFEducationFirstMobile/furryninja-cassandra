import json
import unittest
import datetime
from furryninja import Model, StringProperty, FilterNode, key_ref
from .query import CassandraQuery
from furryninja.query import QueryException

__author__ = 'broken'


class ImageAsset(Model):
    title = StringProperty()
    description = StringProperty()


class TestCassandraQuery(unittest.TestCase):
    def test_query_to_select_statement(self):
        qry = ImageAsset.query()
        cassandra_qry = CassandraQuery(qry).select()

        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset LIMIT 50')

        qry = ImageAsset.query().limit(42)
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset LIMIT 42')

        qry.offset(10)
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset LIMIT 42 OFFSET 10')

        qry.offset(0).limit(50)
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset LIMIT 50')

    def test_query_to_select_statement_with_fields(self):
        qry = ImageAsset.query()
        cassandra_qry = CassandraQuery(qry).select(fields=['key', 'title'])

        self.assertEqual(cassandra_qry.statement, 'SELECT key, title FROM imageasset LIMIT 50')

    def test_query_to_select_statement_with_equal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title == 'Lorem Ipsum')
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE title = %(title)s LIMIT 50')

        qry = ImageAsset.query(ImageAsset.title == 'Lorem Ipsum', ImageAsset.description == 'Lorem')
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE title = %(title)s AND description = %(description)s LIMIT 50')

    def test_query_to_select_statement_with_noequal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title != 'Lorem Ipsum')
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE title != %(title)s LIMIT 50')

    def test_query_to_select_statement_with_less_then_comparison(self):
        qry = ImageAsset.query(ImageAsset.title < 'Lorem Ipsum')
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE title < %(title)s LIMIT 50')

    def test_query_to_select_statement_with_less_equal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title <= 'Lorem Ipsum')
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE title <= %(title)s LIMIT 50')

    def test_query_to_select_statement_with_greater_then_comparison(self):
        qry = ImageAsset.query(ImageAsset.title > 'Lorem Ipsum')
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE title > %(title)s LIMIT 50')

    def test_query_to_select_statement_with_greater_equal_comparison(self):
        qry = ImageAsset.query(ImageAsset.title >= 'Lorem Ipsum')
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE title >= %(title)s LIMIT 50')

    def test_query_to_select_statement_with_key_comparison(self):
        class AudioAsset(Model):
            _key_property_name = 'id'

            @key_ref
            def key(self):
                return getattr(self, self._key_property_name)

        image_test_key = ImageAsset().key
        audio_test_key = AudioAsset().key

        qry = ImageAsset.query(ImageAsset.key == image_test_key)
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE key = %(key)s LIMIT 50')

        qry = AudioAsset.query(AudioAsset.key == audio_test_key)
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM audioasset WHERE key = %(key)s LIMIT 50')

    def test_query_with_list(self):
        class AudioAsset(Model):
            titles = StringProperty(repeated=True)

        qry = ImageAsset.query(AudioAsset.titles.IN(['Here', 'There']))
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM imageasset WHERE titles IN %(titles)s LIMIT 50')

        with self.assertRaises(QueryException):
            ImageAsset.query(AudioAsset.titles == ['Here', 'There'])

    def test_query_with_set_table_str(self):
        class VideoAsset(Model):
            table_str = 'asset'

        qry = VideoAsset.query()
        cassandra_qry = CassandraQuery(qry).select()
        self.assertEqual(cassandra_qry.statement, 'SELECT * FROM asset LIMIT 50')

    def test_insert_query(self):
        asset_data = {
            'title': 'Lorem',
            'description': 'Ipsum'
        }
        asset = ImageAsset()
        qry = asset.query()
        cassandra_qry = CassandraQuery(qry).insert({
            'key': asset.key.urlsafe(),
            'blob': json.dumps(asset_data)
        })

        self.assertEqual(cassandra_qry.statement, 'INSERT INTO imageasset (blob, key) VALUES (%(blob)s, %(key)s)')

    def test_insert_query_if_not_exists(self):
        asset_data = {
            'title': 'Lorem',
            'description': 'Ipsum'
        }
        asset = ImageAsset()
        qry = asset.query()
        cassandra_qry = CassandraQuery(qry).insert({
            'key': asset.key.urlsafe(),
            'blob': json.dumps(asset_data)
        }).if_not_exists()

        self.assertEqual(cassandra_qry.statement, 'INSERT INTO imageasset (blob, key) VALUES (%(blob)s, %(key)s) if not exists')

    def test_update_query(self):
        asset_data = {
            'title': 'Lorem',
            'description': 'Ipsum'
        }
        asset = ImageAsset()
        last_update = str(datetime.datetime.now())
        qry = asset.query(ImageAsset.key == asset.key)
        cassandra_qry = CassandraQuery(qry).update({
            'blob': json.dumps(asset_data),

            'last_update': last_update
        })

        self.assertEqual(cassandra_qry.statement, 'UPDATE imageasset SET blob = %(blob)s, last_update = %(last_update)s WHERE key = %(key)s')

    def test_update_query_if_field(self):
        asset_data = {
            'title': 'Lorem',
            'description': 'Ipsum'
        }
        asset = ImageAsset()
        last_update = str(datetime.datetime.now())
        qry = asset.query(ImageAsset.key == asset.key)
        cassandra_qry = CassandraQuery(qry).update({
            'blob': json.dumps(asset_data),
            'last_update': last_update
        }).update_if('update_token', 'abcdef')

        self.assertEqual(cassandra_qry.statement, 'UPDATE imageasset SET blob = %(blob)s, last_update = %(last_update)s WHERE key = %(key)s if update_token = %(update_token)s')
        self.assertEqual(cassandra_qry.condition_values['update_token'], 'abcdef')