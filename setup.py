from setuptools import setup

setup(
    name='furryninja_cassandra',
    version='0.6.0',
    packages=[
        'furryninja_cassandra',
    ],
    url='',
    license='',
    author='ctx',
    author_email='ctx@ef.com',
    description='',
    install_requires=[
        'cassandra-driver',
        'cql==1.4.0',
        'blist==1.3.6',
        'tornado==4.5.1',
        'furryninja==0.5.0'
    ],
    tests_require=[
        'mock==1.0.1',
        'nose==1.3.3',
        'pysandra-unit==0.5',
    ],
    dependency_links=[
        'git+ssh://git@github.com/EFEducationFirstMobile/furry-ninja.git@v0.5.0#egg=furryninja',
        'git+https://github.com/Zemanta/pysandra-unit.git@fea_cassandra_unit_20#egg=pysandra-unit-0.5'
    ]
)
