#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-postgres",
    version="1.1.3",
    description="Singer.io target for Postgres",
    author="Statsbot",
    url="https://statsbot.co",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_postgres"],
    install_requires=[
        "singer-python==5.1.1",
        "psycopg2==2.8.4",
        "inflection==0.3.1"
    ],
    entry_points="""
    [console_scripts]
    target-postgres=target_postgres:main
    """,
    packages=["target_postgres"],
    package_data = {},
    include_package_data=True,
)
