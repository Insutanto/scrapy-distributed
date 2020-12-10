#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
from pkgutil import walk_packages
from setuptools import setup, find_packages


def read_file(filename):
    with io.open(filename) as fp:
        return fp.read().strip()


def read_rst(filename):
    # Ignore unsupported directives by pypi.
    content = read_file(filename)
    return "".join(
        line for line in io.StringIO(content) if not line.startswith(".. comment::")
    )


setup(
    name="Scrapy-Distributed",
    version="2020.12.1",
    url="https://github.com/Insutanto/scrapy-distributed",
    project_urls={
        "Documentation": "https://github.com/Insutanto/scrapy-distributed",
        "Source": "https://github.com/Insutanto/scrapy-distributed",
        "Tracker": "https://github.com/Insutanto/scrapy-distributed/issues",
    },
    description="A series distributed components for Scrapy framework",
    long_description=read_file("README.md"),
    long_description_content_type="text/markdown",
    author="Insutanto",
    maintainer="Insutanto",
    maintainer_email="insutantow@gmail.com",
    license="BSD",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.6",
    install_requires=[
        "scrapy>=1.8.0",
        "pika>=1.0.0",
        "redisbloom>=0.2.0",
        "redis>=3.0.1",
        "kafka-python>=1.4.7"
    ],
)
