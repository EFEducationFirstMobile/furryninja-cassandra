from setuptools import setup

setup(
    name='furryninja_cassandra',
    version='0.4.0',
    packages=[
        'furryninja_cassandra',
    ],
    url='',
    license='',
    author='ctx',
    author_email='ctx@ef.com',
    description='',
    install_requires=[
        'cassandra-driver==2.1.0',
        'cql==1.4.0',
        'blist==1.3.6',
        'furryninja==0.4.4'
    ],
    tests_require=[
        'mock==1.0.1',
        'nose==1.3.3',
        'pysandra-unit==0.5',
    ],
    dependency_links=[
        'git+ssh://git@github.com/EFEducationFirstMobile/furry-ninja.git@v0.4.4#egg=furryninja-0.4.4',
        'git+https://github.com/Zemanta/pysandra-unit.git@fea_cassandra_unit_20#egg=pysandra-unit-0.5'
    ]
)
