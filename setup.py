#!/usr/bin/env python

"""Setup script for the package."""

import logging
import os
import sys
from typing import Optional

import setuptools

PACKAGE_NAME = "realtime"
MINIMUM_PYTHON_VERSION = "3.7"


def check_python_version() -> None:
    """Exit when the Python version is too low."""
    if sys.version < MINIMUM_PYTHON_VERSION:
        sys.exit("Python {0}+ is required.".format(MINIMUM_PYTHON_VERSION))


def read_package_variable(key: str, filename: str = "__init__.py") -> "Optional[str]":
    """Read the value of a variable from the package without importing."""
    module_path = os.path.join("src", PACKAGE_NAME, filename)
    with open(module_path) as module:
        for line in module:
            parts = line.strip().split(" ", 2)
            if parts[:-1] == [key, "="]:
                return parts[-1].strip("'").strip('"')
    logging.warning("'%s' not found in '%s'", key, module_path)
    return None


check_python_version()


DEV_REQUIRES = [
    "asyncpg",
    "black",
    "mypy",
    "neovim",
    "psycopg2-binary",
    "pylint",
    "pytest",
    "pytest-asyncio",
    "pytest-cov",
    "pytest-timeout",
    "sqlalchemy[mypy]>=1.4",
]

setuptools.setup(
    name=read_package_variable("__project__"),
    version=read_package_variable("__version__"),
    description="Realtime Subscription to PostgreSQL for Python",
    url="https://github.com/olirice/realtime",
    author="Oliver Rice",
    author_email="oliver@oliverrice.com",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=["sqlalchemy>=1.4"],
    extras_require={
        "dev": DEV_REQUIRES,
        "docs": ["sphinx", "sphinx_rtd_theme"],
    },
)
