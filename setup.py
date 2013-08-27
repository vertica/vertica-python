#!/usr/bin/env python
import collections
from setuptools import setup

from pip.req import parse_requirements

dependency_links = []
install_requires = []

ReqOpts = collections.namedtuple('ReqOpts', ['skip_requirements_regex', 'default_vcs'])

opts = ReqOpts(None, 'git')

for ir in parse_requirements("requirements.txt", options=opts):
    if ir is not None:
        if ir.url is not None:
            dependency_links.append(str(ir.url))
        if ir.req is not None:
            install_requires.append(str(ir.req))

setup(
    name='vertica-python',
    version='0.1.1',
    description='A native Python client for the Vertica database.',
    author='Justin Berka',
    author_email='justin@uber.com',
    url='https://github.com/uber/vertica-python/',
    keywords="database vertica",
    packages=['vertica_python'],
    include_pckage_data=True,
    license="MIT",
    install_requires=install_requires,
    dependency_links=dependency_links,
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Operating System :: OS Independent"
    ]
)
