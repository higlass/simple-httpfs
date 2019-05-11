#!/usr/bin/env python3
from setuptools import setup, find_packages
import versioneer

setup(
    name='simple-httpfs',
    author='Peter Kerpedjiev',
    author_email='pkerpedjiev@gmail.com',
    packages=['simple_httpfs'],
    entry_points={
      'console_scripts': [
          'simple-httpfs = simple_httpfs.__main__:main'
      ]
    },
    url='https://github.com/higlass/simple-httpfs',
    description='A simple FUSE filesystem for reading http files',
    license='MIT',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        "fusepy",
        "requests",
        "diskcache"
    ],
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass()
)
