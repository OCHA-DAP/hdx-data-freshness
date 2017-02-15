# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

requirements = ['aiodns',
                'aiohttp',
                'cchardet',
                'psycopg2',
                'tqdm'
                ]

setup(
    name='hdx-data-freshness',
    version='0.1',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    url='http://data.humdata.org/',
    license='PSF',
    author='Michael Rans',
    author_email='rans@email.com',
    description='HDX Data Freshness',
    install_requires=requirements,
    package_data={
        # Include any package contains *.yml files, include them:
        '': ['*.yml'],
    },
    include_package_data=True,
)
