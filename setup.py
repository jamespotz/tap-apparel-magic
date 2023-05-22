#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-apparel-magic",
    version="0.1.1",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_apparel_magic"],
    install_requires=[
        "singer-python==5.8.0",
        "requests==2.31.0",
        "python-dateutil==2.8.1",
        "attrs==19.3.0"
    ],
    entry_points="""
    [console_scripts]
    tap-apparel-magic=tap_apparel_magic:main
    """,
    packages=["tap_apparel_magic"],
    package_data = {
        "schemas": ["tap_apparel_magic/schemas/*.json"]
    },
    include_package_data=True,
)
