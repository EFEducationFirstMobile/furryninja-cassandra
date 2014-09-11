from collections import OrderedDict
import copy
import functools
import json
from string import lower
import unittest
from cassandra.query import ValueSequence
import mock
import pytz
from furryninja import KeyProperty, AttributesProperty, IntegerProperty, StringProperty, Model, Key, computed_property
from furryninja import QueryNotFoundException
from .repository import CassandraRepository, Edge
from furryninja.model import DateTimeProperty

__author__ = 'broken'

IMAGE_ASSET = {
    'name': 'Written speech',
    'description': 'Lorem ipsum dolor sit amet, consectetur adipisici elit',
    'title': 'Lorem Ipsum',
    'skills': [
        'flying',
        'superpowers'
    ],
    'publicationDate': {
        'year': 2000
    },
    'topics': [
        'G9dCxjCen-3J6OwdwAmD8pO'
    ],
    'attributes': {
        'imageFormat': [
            'G9dCxjCen-3J6OwdwAmD8pO',
            'G9dCxjCen-oJQxOYorByPjm'
        ],
        'imageType': [
            'G9dCxjCen-3J6OwdwAmD8pO'
        ],
        'files': [{
            'url': 'http://goo.gl'
        }]
    }
}


class Tag(Model):
    title = StringProperty()


class ImageAsset(Model):
    default_fields = ['topics', 'attributes.imageFormat', 'attributes.imageType']

    title = StringProperty()
    description = StringProperty()
    name = StringProperty()
    skills = StringProperty(repeated=True)

    topics = KeyProperty(kind=Tag, repeated=True)

    publicationDate = AttributesProperty(attributes={
        'year': IntegerProperty()
    })

    attributes = AttributesProperty(attributes={
        'imageFormat': KeyProperty(kind=Tag, repeated=True),
        'imageType': KeyProperty(kind=Tag, repeated=True),
        'files': AttributesProperty(attributes={
            'url': StringProperty(),
            'bucket_key': StringProperty(),
            'mime_type': StringProperty(),
            'height': IntegerProperty(),
            'width': IntegerProperty()
        }, repeated=True)
    })


class TestCassandraRepository(unittest.TestCase):
    def setUp(self):
        def conn(*args):
            return mock.Mock(**{
                'set_core_connections_per_host.return_value': None,
                'connect.return_value': mock.Mock(**{
                    'prepare.return_value': None,
                    'execute.return_value': mock.Mock()
                })
            })

        connection = functools.partial(conn)

        self.repo = CassandraRepository(connection_class=connection)

    def test_denormalize(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        expected = dict(IMAGE_ASSET.items() + {'key': image.key.urlsafe()}.items())
        self.assertDictEqual(self.repo.denormalize(image), expected)

    def test_denormalize_with_date(self):
        class Book(Model):
            published = DateTimeProperty(auto_now_add=True)
            updated = DateTimeProperty(auto_now=True)

        book = Book()
        self.assertDictEqual(self.repo.denormalize(book), {
            'key': book.key.urlsafe(),
            'published': book.published.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
            'updated': book.updated.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        })

        json.dumps(self.repo.denormalize(book))

    def test_find_edges(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        expected = self.repo.find_edges(image)

        self.assertEqual(expected[0].label, 'attributes.imageType')
        self.assertEqual(expected[1].label, 'attributes.imageFormat')
        self.assertEqual(expected[2].label, 'topics')
        self.assertEqual(expected[3].label, 'attributes.imageFormat')

        duplicate_image_format = copy.deepcopy(IMAGE_ASSET)
        duplicate_image_format['attributes']['imageFormat'] = [
            'G9dCxjCen-oJQxOYorByPjm',
            'G9dCxjCen-oJQxOYorByPjm'
        ]

        image = ImageAsset(**duplicate_image_format)
        expected = self.repo.find_edges(image)

        self.assertEqual(expected[0].label, 'attributes.imageType')
        self.assertEqual(expected[1].label, 'topics')
        self.assertEqual(expected[2].label, 'attributes.imageFormat')

    def test_find_edges_with_complex_model(self):
        class Entity(Model):
            ref = KeyProperty(kind=Tag)
            relations = AttributesProperty(attributes={
                'asset': KeyProperty(kind=Tag)
            })
            assets = AttributesProperty(repeated=True, attributes={
                'asset': KeyProperty(kind=Tag)
            })

        self.repo.find_edges(Entity())

    def test_create_edges(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        edges = self.repo.find_edges(image)

        self.repo.set_edges_for_model(image, edges)
        self.assertEqual(self.repo.session.execute.call_count, 4)

        with mock.patch.object(self.repo, 'insert_edge') as insert_edge:
            self.repo.set_edges_for_model(image, edges)
            self.assertEqual(insert_edge.call_count, 4)

            self.assertEqual(insert_edge.call_args[0][0].indoc.urlsafe(), image.key.urlsafe())
            self.assertEqual(insert_edge.call_args[0][0].outdoc.urlsafe(), edges[3].outdoc.urlsafe())

    def test_create_model(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))

        self.repo.insert(image)
        self.maxDiff = None
        self.assertEqual(self.repo.session.execute.call_count, 5)
        self.assertEqual(self.repo.session.execute.call_args_list[0], mock.call('INSERT INTO imageasset (blob, key) VALUES (%(blob)s, %(key)s)', parameters={
            'key': image.key.urlsafe(),
            'blob': json.dumps(self.repo.denormalize(image))
        }))

    def test_get_model(self):
        class Entity(Model):
            title = StringProperty()
            description = StringProperty()

        en = Entity()
        en_str_key = en.key.urlsafe()
        self.repo.session.execute.side_effect = [
            [
                {
                    'key': en_str_key,
                    'title': 'Lorem ipsum',
                    'description': 'Lorem ipsum'
                }
            ]
        ]

        fetched_entity = self.repo.get(en)

        self.assertEqual(fetched_entity.title, 'Lorem ipsum')
        self.assertEqual(self.repo.session.execute.call_count, 1)
        self.assertEqual(self.repo.session.execute.call_args_list[0], mock.call('SELECT * FROM entity WHERE key = %(key)s LIMIT 1', parameters={
            'key': en.key.urlsafe()
        }))

    def test_get_not_found_model(self):
        class Entity(Model):
            title = StringProperty()
            description = StringProperty()

        en = Entity()
        self.repo.session.execute.side_effect = [
            []
        ]

        with self.assertRaises(QueryNotFoundException):
            self.repo.get(en)

    def test_get_model_with_blob(self):
        tag = [{
            'title': 'Hello, earth!'
        }]

        image = ImageAsset()
        self.repo.session.execute.side_effect = [
            [
                {
                    'key': 'DD0H97clzCKLuQXSe9cLEHgrH5KI9Q-kPeweAB1D1Y5l',
                    'blob': json.dumps(IMAGE_ASSET)
                }
            ],
            tag,
            tag,
            tag,
            tag
        ]

        self.repo.get(image)
        self.maxDiff = None
        self.assertEqual(self.repo.session.execute.call_count, 5)
        self.assertEqual(self.repo.session.execute.call_args_list[0], mock.call('SELECT * FROM imageasset WHERE key = %(key)s LIMIT 1', parameters={
            'key': image.key.urlsafe()
        }))

    def test_update_model(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        self.repo.session.execute.side_effect = [
            True,
            [],
            True,
            True,
            True,
            True
        ]

        self.repo.update(image)
        self.maxDiff = None
        self.assertEqual(self.repo.session.execute.call_count, 6)
        self.assertEqual(self.repo.session.execute.call_args_list[0], mock.call('UPDATE imageasset SET blob = %(blob)s WHERE key = %(key)s', parameters={
            'key': image.key.urlsafe(),
            'blob': json.dumps(self.repo.denormalize(image))
        }))

    def test_update_edges(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))

        edges = self.repo.find_edges(image)
        self.repo.set_edges_for_model(image, edges, [])
        self.assertEqual(self.repo.session.execute.call_count, 4)

        # Remove one will result in no new edges and one delete. So total call count should be 5
        removed_edge = image.attributes.imageFormat.pop()
        existing_edges = edges
        existing_edges[-1] = Edge(**{
            'key': 'EJLCVyC5AUEQ-AxpVmA5oQwRWZ',
            'label': existing_edges[-1].label,
            'outdoc': existing_edges[-1].outdoc
        })

        edges = self.repo.find_edges(image)

        self.repo.set_edges_for_model(image, edges, existing_edges)
        self.assertEqual(self.repo.session.execute.call_count, 5)

        # self.assertEqual(self.repo.session.execute.call_args[0], ('DELETE FROM edge WHERE indoc IN %(indoc)s AND outdoc IN %(outdoc)s AND label IN %(label)s', ))
        # self.assertTrue(isinstance(self.repo.session.execute.call_args[1]['parameters']['indoc'], ValueSequence))
        # self.assertTrue(isinstance(self.repo.session.execute.call_args[1]['parameters']['outdoc'], ValueSequence))
        # self.assertTrue(isinstance(self.repo.session.execute.call_args[1]['parameters']['label'], ValueSequence))

        # Adding one will result in 1 new edges and 0 delete. So total call count should be 6
        image.attributes.imageFormat.append(removed_edge)
        existing_edges = edges
        edges = self.repo.find_edges(image)

        self.repo.set_edges_for_model(image, edges, existing_edges)
        self.assertEqual(self.repo.session.execute.call_count, 6)

    def test_delete_model(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))

        edges = self.repo.find_edges(image)

        self.repo.session.execute.return_value = [
            Edge(**{
                'key': 'EJLCVyC5AUEQ-AxpVmA5oQwRWZ',
                'label': edges[-1].label,
                'outdoc': edges[-1].outdoc
            })
        ]

        self.repo.delete(image)
        self.maxDiff = None
        self.assertEqual(self.repo.session.execute.call_count, 3)
        self.assertEqual(self.repo.session.execute.call_args_list[0], mock.call('SELECT * FROM edge WHERE indoc = %(indoc)s LIMIT 50', parameters={
            'indoc': image.key.urlsafe()
        }))
        # self.assertEqual(self.repo.session.execute.call_args_list[1][0], ('DELETE FROM edge WHERE indoc IN %(indoc)s AND outdoc IN %(outdoc)s AND label IN %(label)s', ))
        # self.assertTrue(isinstance(self.repo.session.execute.call_args_list[1][1]['parameters']['indoc'], ValueSequence))
        # self.assertTrue(isinstance(self.repo.session.execute.call_args_list[1][1]['parameters']['outdoc'], ValueSequence))
        # self.assertTrue(isinstance(self.repo.session.execute.call_args_list[1][1]['parameters']['label'], ValueSequence))
        self.assertEqual(self.repo.session.execute.call_args_list[2], mock.call('DELETE FROM imageasset WHERE key = %(key)s', parameters={
            'key': image.key.urlsafe()
        }))

    def test_fetch_query(self):
        tag = [{
            'title': 'Hello, earth!'
        }]

        qry = ImageAsset.query()
        self.repo.session.execute.side_effect = [
            [
                {
                    'key': 'DD0H97clzCKLuQXSe9cLEHgrH5KI9Q-kPeweAB1D1Y5l',
                    'blob': json.dumps(IMAGE_ASSET)
                },
                {
                    'key': 'DD0H97clzCKLuQXSe9cLEHgrH5KI9Q-kPeweAB1D1Y5l',
                    'blob': json.dumps(IMAGE_ASSET)
                }
            ],
            tag,
            tag,
            tag,
            tag,
            tag,
            tag,
            tag,
            tag
        ]

        self.repo.fetch(qry)

        self.assertEqual(self.repo.session.execute.call_count, 9)
        self.assertEqual(self.repo.session.execute.call_args_list[0][0], ('SELECT * FROM imageasset LIMIT 50', ))

    def test_model_pre_put_hook(self):
        class VideoAsset(Model):
            music = 'rock'

            def _pre_put_hook(self):
                self.music = 'metal'

        video = VideoAsset()
        self.assertEqual(video.music, 'rock')
        self.repo.insert(video)
        self.assertEqual(video.music, 'metal')

        video2 = VideoAsset()
        self.assertEqual(video2.music, 'rock')
        self.repo.session.execute.side_effect = [
            True,
            []
        ]
        self.repo.update(video2)
        self.assertEqual(video2.music, 'metal')

    def test_model_post_put_hook(self):

        class VideoAsset(Model):
            music = 'rock'

            def _post_put_hook(self):
                self.music = 'metal'

        video = VideoAsset()
        self.assertEqual(video.music, 'rock')
        self.repo.insert(video)
        self.assertEqual(video.music, 'metal')

        video2 = VideoAsset()
        self.assertEqual(video2.music, 'rock')

        self.repo.session.execute.side_effect = [
            True,
            []
        ]

        self.repo.update(video2)
        self.assertEqual(video2.music, 'metal')