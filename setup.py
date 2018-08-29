#!/usr/bin/env python
from setuptools import setup

with open('README.rst', 'r') as f:
    long_description = f.read()


setup(name='influx-sansio',
      version='0.1.1',
      description='SansIO Python client for InfluxDB',
      long_description=long_description,
      author='Michael Elsdorfer',
      author_email='michael@elsdoerfer.com',
      url='https://github.com/miracle2k/influx-sansio',
      packages=['influx_sansio'],
      include_package_data=True,
      python_requires='>=3.6',
      install_requires=['pandas>=0.21',
                        'numpy',
                        ],
      extras_require={'test': ['pytest',
                               'pytest-asyncio',
                               'pytest-cov',
                               'pyyaml']},
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.6',
          'Topic :: Database',
      ])
