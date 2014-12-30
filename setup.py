#!/usr/bin/env python
from setuptools import setup

# get version
from memsql_loader import __version__

setup(
    name='memsql_loader',
    version=__version__,
    author='MemSQL',
    author_email='support@memsql.com',
    url='http://github.com/memsql/memsql-loader',
    license='LICENSE.txt',
    description='MemSQL Loader helps you run complex ETL workflows against MemSQL',
    long_description=open('README.md').read(),
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    scripts=[ 'bin/memsql-loader' ],
    packages=[
        'memsql_loader',
        'memsql_loader.cli',
        'memsql_loader.db',
        'memsql_loader.execution',
        'memsql_loader.loader_db',
        'memsql_loader.util',
    ],
    zip_safe=False,
    install_requires=[
        'memsql==2.14.4',
        'wraptor==0.6.0',
        'clark==0.1.0',
        'voluptuous==0.8.5',
        'boto==2.28.0',
        'pycurl==7.19.3.1',
        'prettytable==0.7.2',
        'pywebhdfs==0.2.4'
    ],
    tests_require=[
        'docker-py==0.3.1',
        'pytest==2.5.2',
        'pytest-xdist==1.10',
        'pexpect==3.3'
    ],
)
