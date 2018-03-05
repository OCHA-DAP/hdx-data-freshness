# -*- coding: utf-8 -*-
import inspect
import sys
from codecs import open
from os.path import join, abspath, realpath, dirname

from setuptools import setup, find_packages


def script_dir(pyobject, follow_symlinks=True):
    """Get current script's directory

    Args:
        pyobject (Any): Any Python object in the script
        follow_symlinks (Optional[bool]): Follow symlinks or not. Defaults to True.

    Returns:
        str: Current script's directory
    """
    if getattr(sys, 'frozen', False):  # py2exe, PyInstaller, cx_Freeze
        path = abspath(sys.executable)
    else:
        path = inspect.getabsfile(pyobject)
    if follow_symlinks:
        path = realpath(path)
    return dirname(path)


def script_dir_plus_file(filename, pyobject, follow_symlinks=True):
    """Get current script's directory and then append a filename

    Args:
        filename (str): Filename to append to directory path
        pyobject (Any): Any Python object in the script
        follow_symlinks (Optional[bool]): Follow symlinks or not. Defaults to True.

    Returns:
        str: Current script's directory and with filename appended
    """
    return join(script_dir(pyobject, follow_symlinks), filename)


def get_readme():
    readme_file = open(script_dir_plus_file('README.rst', get_readme), encoding='utf-8')
    return readme_file.read()


requirements = ['aiodns',
                'aiohttp==2.3.9',
                'psycopg2',
                'tqdm',
                'uvloop',
                'hdx-python-api>=2.8.8'
                ]

classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Natural Language :: English",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

setup(
    name='hdx-data-freshness',
    description='HDX Data Freshness',
    license='MIT',
    url='https://github.com/OCHA-DAP/hdx-data-freshness',
    version='1.0.6',
    author='Michael Rans',
    author_email='rans@email.com',
    keywords=['HDX', 'fresh', 'freshness', 'data freshness'],
    long_description=get_readme(),
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    zip_safe=True,
    classifiers=classifiers,
    install_requires=requirements,
)
