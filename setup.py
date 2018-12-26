#!/usr/bin/env python3
from setuptools import setup, find_packages
import versioneer

setup(
    name='simple-httpfs',
    author='Peter Kerpedjiev',
    author_email='pkerpedjiev@gmail.com',
    packages=[ 'simple_httpfs' ],
    scripts=['simple_httpfs/simple-httpfs.py'],
    url='https://github.com/higlass/simple-httpfs',
    license='LICENSE.txt',
    description='A simple FUSE filesystem for reading http files',
    long_description=open('README.md').read(),
    install_requires=[
        "fusepy",
        "requests",
        "diskcache"
    ],
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass()
)
