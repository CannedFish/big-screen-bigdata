#!/usr/bin/python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

import os.path
import imp
import sys

def main():
    HERE = os.path.abspath(os.path.dirname(__file__))

    setup = os.path.join(HERE, "cm_api", "setup.py")
    fobj = open(setup)
    imp.load_module("__setup2__", fobj, setup, imp.get_suffixes()[3])
    
    sys.path.insert(0, os.path.join(HERE, 'etl'))

    setup_args = dict(
        name="big_screen_bigdata",
        version="0.5.0",
        description="ETL for preparing bigdata statistic for big screen",
        author="CannedFish Liang",
        author_email="lianggy0719@126.com",
        url="https://github.com/CannedFish/big_screen_bigdata",
        platforms="Linux",
        license="BSD",
        packages=find_packages(),
        install_requires=['cm_api==17.0.0'],
        package_data={},
        entry_points={
            'console_scripts': [
                'bg_bigdata_etl = etl.main:main',
            ]
        }
    )
    setup(**setup_args)

if __name__ == '__main__':
    main()

