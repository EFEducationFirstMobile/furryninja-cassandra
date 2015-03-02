import copy
import json
import unittest
from cassandra import ConsistencyLevel
from pysandraunit.testcasebase import CassandraTestCaseBase
import mock
import pytz
from furryninja import KeyProperty, AttributesProperty, IntegerProperty, StringProperty, Model, Key, key_ref
from furryninja.model import DateTimeProperty
from furryninja import Settings
from furryninja import QueryNotFoundException
from furryninja_cassandra.query import CassandraQuery
from .repository import CassandraRepository, Edge
from .model import CassandraModelMixin

__author__ = 'broken'


class PYSANDRASettings:
    PYSANDRA_SCHEMA_FILE_PATH = '/Users/broken/ef/development/furryninja-cassandra/test_config/cassandra.schema.cql'
    PYSANDRA_TMP_DIR = '/tmp/cassandratmp'
    PYSANDRA_CASSANDRA_YAML_OPTIONS = {}

CassandraTestCaseBase.set_global_settings(PYSANDRASettings)


Settings.set('db', {
    'name': 'test_keyspace',
    'port': '9142',
    'host': ['localhost'],
    'protocol_version': 2,
    'consistency_level': ConsistencyLevel.SERIAL
})


IMAGE_ASSET = {
    'name': 'Written speech',
    'description': 'Lorem ipsum dolor sit amet, consectetur adipisici elit',
    'title': 'Lorem Ipsum',
    'version': '42',
    'skills': [
        'flying',
        'superpowers'
    ],
    'publicationDate': {
        'year': 2000
    },
    'topics': [
        'G9dCxjCen-oJMYWaN2vjn18'
    ],
    'attributes': {
        'imageFormat': [
            'G9dCxjCen-4QD1ydlavEYj4',
            'G9dCxjCen-P5Ep0LbmbM7yy'
        ],
        'imageType': [
            'G9dCxjCen-N5EXYgamnvPVn'
        ],
        'files': [{
            'url': 'http://goo.gl'
        }]
    }
}


class TestModelMixin(CassandraModelMixin):
    _storage_type = ('json', 'blob')

    @key_ref
    def kind(self):
        return self.key.kind

    @key_ref
    def revision(self):
        if hasattr(self, 'version'):
            return self.version
        return '1'

    @key_ref
    def update_token(self):
        if hasattr(self, 'version'):
            return self.version
        return '1'


class Tag(Model, TestModelMixin):
    title = StringProperty()


class ImageAsset(Model, TestModelMixin):
    default_fields = ['topics', 'attributes.imageFormat', 'attributes.imageType']

    title = StringProperty()
    version = StringProperty()
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


class Book(Model, TestModelMixin):
    title = StringProperty()
    published = DateTimeProperty(auto_now_add=True)
    updated = DateTimeProperty(auto_now=True)


class VideoAsset(Model, CassandraModelMixin):
    title = StringProperty(default='monkey')
    music = 'rock'
    num = IntegerProperty()

    def _pre_put_hook(self):
        self.music = 'metal'

    @key_ref
    def revision(self):
        if hasattr(self, 'version'):
            return self.version
        return '1'


class TestCassandraRepository(CassandraTestCaseBase, unittest.TestCase):
    def setUp(self):

        self._start_cassandra()
        self.repo = CassandraRepository()

    def tearDown(self):
        self._clean_cassandra()

    def test_denormalize(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        expected = dict(IMAGE_ASSET.items() + {'key': image.key.urlsafe()}.items())
        self.assertDictEqual(json.loads(self.repo.denormalize(image)['blob']), expected)

    def test_denormalize_with_model(self):
        tag = Tag(**{'title': 'A Tag'})
        image = ImageAsset(**{'name': 'Monkey', 'topics': [tag]})

        expected = dict({'name': 'Monkey', 'key': image.key.urlsafe(), 'topics': [tag.key.urlsafe()]}.items())
        self.assertDictEqual(json.loads(self.repo.denormalize(image)['blob']), expected)

    def test_denormalize_with_date(self):
        class Book(Model, CassandraModelMixin):
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

        self.assertEqual(expected[0].label, 'topics')
        self.assertEqual(expected[1].label, 'attributes.imageFormat')
        self.assertEqual(expected[2].label, 'attributes.imageFormat')
        self.assertEqual(expected[3].label, 'attributes.imageType')

        duplicate_image_format = copy.deepcopy(IMAGE_ASSET)
        duplicate_image_format['attributes']['imageFormat'] = [
            'G9dCxjCen-P5Ep0LbmbM7yy',
            'G9dCxjCen-P5Ep0LbmbM7yy'
        ]

        image = ImageAsset(**duplicate_image_format)
        expected = self.repo.find_edges(image)

        self.assertEqual(expected[0].label, 'topics')
        self.assertEqual(expected[1].label, 'attributes.imageFormat')
        self.assertEqual(expected[2].label, 'attributes.imageType')

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

        cql_edges = self.repo.fetch(Edge.query())
        self.assertEqual(len(cql_edges), 4)
        self.assertEqual(cql_edges[0].indoc.urlsafe(), image.key.urlsafe())
        self.assertEqual(cql_edges[0].outdoc.urlsafe(), 'G9dCxjCen-4QD1ydlavEYj4')

        with mock.patch.object(self.repo, 'insert_edge') as insert_edge:
            self.repo.set_edges_for_model(image, edges)
            self.assertEqual(insert_edge.call_count, 4)

            self.assertEqual(insert_edge.call_args[0][0].indoc.urlsafe(), image.key.urlsafe())
            self.assertEqual(insert_edge.call_args[0][0].outdoc.urlsafe(), edges[3].outdoc.urlsafe())

    def test_create_model(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))

        cql_qry = CassandraQuery(ImageAsset.query()).insert({'blob': '<--blob-->', 'key': '<--key-->'})
        self.assertEqual(cql_qry.statement, 'INSERT INTO imageasset (blob, key) VALUES (%(blob)s, %(key)s)')

        self.repo.insert(image)
        self.maxDiff = None

        entities = self.repo.fetch(image.query())
        self.assertEqual(len(entities), 1)

    def test_create_model_if_exists(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        self.repo.insert(image)

        entities = self.repo.fetch(image.query())
        self.assertEqual(len(entities), 1)

        self.repo.insert(image)
        self.assertEqual(len(entities), 1)

    def test_create_multi_model(self):
        images = [
            ImageAsset(**copy.deepcopy(IMAGE_ASSET)),
            ImageAsset(**copy.deepcopy(IMAGE_ASSET)),
            ImageAsset(**copy.deepcopy(IMAGE_ASSET)),
            ImageAsset(**copy.deepcopy(IMAGE_ASSET)),
            ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        ]

        self.repo.insert_multi(images)
        self.maxDiff = None

        entities = self.repo.fetch(images[0].query())
        self.assertEqual(len(entities), 5)

    def test_get_model(self):
        en = Book(**{
            'title': 'Lorem ipsum'
        })

        query = en.query(en.__class__.key == en.key).limit(1)
        cql_qry = CassandraQuery(query).select()
        self.assertEqual(cql_qry.statement, 'SELECT * FROM book WHERE key = %(key)s LIMIT 1')

        self.repo.insert(en)
        self.maxDiff = None

        fetched_entity = self.repo.get(en)

        self.assertEqual(fetched_entity.title, 'Lorem ipsum')

    def test_get_not_found_model(self):
        en = Book()

        with self.assertRaises(QueryNotFoundException):
            self.repo.get(en)

    def test_get_model_with_blob(self):
        tag = Tag(**{
            'key': 'G9dCxjCen-oJMYWaN2vjn18',
            'title': 'Hello, earth!'
        })

        self.repo.insert(tag)

        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        self.repo.insert(image)

        image = self.repo.get(image)
        self.assertEqual(image.topics[0].title, 'Hello, earth!')

    def test_update_model(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        self.repo.insert(image)

        self.assertEqual(image.title, 'Lorem Ipsum')
        image.title = 'Hello, earth!'
        self.repo.update(image)

        updated_image = self.repo.get(image)
        self.assertEqual(updated_image.title, 'Hello, earth!')

    def test_update_model_if_num(self):
        video = VideoAsset(**{'title': 'monkey', 'num': 1})
        self.repo.insert(video)

        video.title = 'Hello, earth!'
        self.repo.update(video, update_if=('num', 2))

        video = self.repo.get(video)
        self.assertEqual(video.title, 'monkey')

        video.title = 'Hello, earth!'
        self.repo.update(video, update_if=('num', 1))

        video = self.repo.get(video)
        self.assertEqual(video.title, 'Hello, earth!')

    def test_update_model_if_str(self):
        video = VideoAsset(**{'title': 'monkey', 'num': 1})
        self.repo.insert(video)

        video.title = 'Hello, earth!'
        self.repo.update(video, update_if=('title', 'apa'))

        video = self.repo.get(video)
        self.assertEqual(video.title, 'monkey')

        video.title = 'Hello, earth!'
        self.repo.update(video, update_if=('title', 'monkey'))

        video = self.repo.get(video)
        self.assertEqual(video.title, 'Hello, earth!')

    def test_update_edges(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))

        edges = self.repo.find_edges(image)
        self.repo.set_edges_for_model(image, edges, [])

        cql_edges = self.repo.fetch(Edge.query())
        self.assertEqual(len(cql_edges), 4)

        removed_edge = image.attributes.imageFormat.pop()
        image.attributes.imageFormat = [image.attributes.imageFormat[0]]
        existing_edges = edges
        existing_edges[-1] = Edge(**{
            'key': 'EJLCVyC5AUEQ-AxpVmA5oQwRWZ',
            'label': existing_edges[-1].label,
            'outdoc': existing_edges[-1].outdoc
        })

        edges = self.repo.find_edges(image)

        self.repo.set_edges_for_model(image, edges, existing_edges)
        cql_edges = self.repo.fetch(Edge.query())
        self.assertEqual(len(cql_edges), 3)

        # Adding one will result in 1 new edges and 0 delete.
        image.attributes.imageFormat = image.attributes.imageFormat + [removed_edge]
        existing_edges = edges
        edges = self.repo.find_edges(image)

        self.repo.set_edges_for_model(image, edges, existing_edges)
        cql_edges = self.repo.fetch(Edge.query())
        self.assertEqual(len(cql_edges), 4)

    def test_delete_model(self):
        image = ImageAsset(**copy.deepcopy(IMAGE_ASSET))
        self.repo.insert(image)

        entities = self.repo.fetch(image.query())
        self.assertEqual(len(entities), 1)

        cql_edges = self.repo.fetch(Edge.query())
        self.assertEqual(len(cql_edges), 4)

        self.repo.delete(image)

        entities = self.repo.fetch(image.query())
        self.assertEqual(len(entities), 0)

        cql_edges = self.repo.fetch(Edge.query())
        self.assertEqual(len(cql_edges), 0)

    def test_fetch_query(self):
        image1 = ImageAsset(**{'title': 'title1'})
        image2 = ImageAsset(**{'title': 'title2'})
        image3 = ImageAsset(**{'title': 'title3'})
        self.repo.insert(image1)
        self.repo.insert(image2)
        self.repo.insert(image3)

        entities = self.repo.fetch(ImageAsset.query())
        self.assertEqual(len(entities), 3)

        entities = self.repo.fetch(ImageAsset.query().limit(1))
        self.assertEqual(len(entities), 1)

    def test_model_pre_put_hook(self):
        video = VideoAsset(**{'title': 'monkey'})
        self.assertEqual(video.music, 'rock')
        self.repo.insert(video)
        self.assertEqual(video.music, 'metal')

        video2 = VideoAsset(**{'title': 'monkey'})
        self.assertEqual(video2.music, 'rock')

        self.repo.update(video2)
        self.assertEqual(video2.music, 'metal')

    def test_model_post_put_hook(self):
        video = VideoAsset(**{'title': 'monkey'})
        self.assertEqual(video.music, 'rock')
        self.repo.insert(video)
        self.assertEqual(video.music, 'metal')

        video2 = VideoAsset(**{'title': 'monkey'})
        self.assertEqual(video2.music, 'rock')

        self.repo.update(video2)
        self.assertEqual(video2.music, 'metal')