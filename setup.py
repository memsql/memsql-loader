#!/usr/bin/env python
from setuptools import setup

# get version
from memsql_loader import __version__

setup(
    name='memsql-loader',
    version=__version__,
    author='MemSQL',
    author_email='support@memsql.com',
    url='https://github.com/memsql/memsql-loader',
    download_url='https://github.com/memsql/memsql-loader/releases/latest',
    license='LICENSE.txt',
    description='MemSQL Loader helps you run complex ETL workflows against MemSQL',
    long_description=open('README.md').read(),
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    platforms=[ "Linux", "Mac OS X" ],
    entry_points={
        'console_scripts': [
            'memsql-loader = memsql_loader.main:main'
        ]
    },
    packages=[
        'memsql_loader',
        'memsql_loader.api',
        'memsql_loader.cli',
        'memsql_loader.db',
        'memsql_loader.execution',
        'memsql_loader.loader_db',
        'memsql_loader.util',
        'memsql_loader.util.apsw_sql_step_queue',
        'memsql_loader.vendor',
        'memsql_loader.vendor.glob2',
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
        'pywebhdfs==0.3.2',
        'requests==2.5.1',
    ],
    tests_require=[
        'docker-py==0.3.1',
        'pytest==2.5.2',
        'pytest-xdist==1.10',
        'pexpect==3.3',
        'requests==2.2.1',
    ],
)
