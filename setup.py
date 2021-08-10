#!/usr/bin/env python

import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name="hh_parser",
    version="1.0.0",
    description="Parse hh vacancies and send them to Telegram",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/kkecher/hh_parser",
    author="Ivan Arzhanov",
    author_email="kkecher@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3"
    ],
    python_requires=">=3.6.0",
    install_requires=["cryptography", "keyring", "requests"]
    },
)
