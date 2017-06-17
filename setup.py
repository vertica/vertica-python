#!/usr/bin/env python
import collections
from setuptools import setup, find_packages


ReqOpts = collections.namedtuple('ReqOpts', ['skip_requirements_regex', 'default_vcs'])

opts = ReqOpts(None, 'git')

# version should use the format 'x.x.x' (instead of 'vx.x.x')
setup(
    name='vertica-python',
    version='0.7.2',
    description='A native Python client for the Vertica database.',
    author='Justin Berka, Alex Kim',
    author_email='justin.berka@gmail.com, alex.kim@uber.com',
    url='https://github.com/uber/vertica-python/',
    keywords="database vertica",
    packages=find_packages(),
    license="MIT",
    install_requires=[
        'python-dateutil>=1.5',
        'pytz',
        'future',
        'six>=1.10.0'
    ],
    extras_require={'namedparams': ['psycopg2>=2.5.1']},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Operating System :: OS Independent"
    ]
)
