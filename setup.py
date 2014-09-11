from setuptools import setup

setup(
    name='furryninja_cassandra',
    version='0.1.0',
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
        'furryninja'
    ],
    dependency_links=[
        'https://github.com/EFEducationFirstMobile/furry-ninja/tarball/master#egg=furryninja'
    ]
)
