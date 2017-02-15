# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

requirements = ['aiodns',
                'aiohttp',
                'cchardet',
                'tqdm',
                'hdx-python-api==0.81'
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
    dependency_links=[
        'https://github.com/ocha-dap/hdx-python-api/archive/master.zip#egg=hdx-python-api-0.81'
    ],
    install_requires=requirements,
    package_data={
        # Include any package contains *.yml files, include them:
        '': ['*.yml'],
    },
    include_package_data=True,
)
