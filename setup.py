from setuptools import setup

setup(
    name='furryninja_cassandra',
    version='0.3.4',
    packages=[
        'furryninja_cassandra',
    ],
    url='',
    license='',
    author='ctx',
    author_email='ctx@ef.com',
    description='',
    install_requires=[
        'mock==1.0.1',
        'nose==1.3.3',
        'cassandra-driver',
        'cql',
        'blist',
        'furryninja==0.4.0',
        'pysandra-unit'
    ],
    dependency_links=[
        'git+ssh://git@github.com/EFEducationFirstMobile/furry-ninja.git@v0.4.0#egg=furryninja-0.4.0'
    ]
)
