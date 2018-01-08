#!/usr/bin/env python
#

from setuptools import setup, find_packages

setup(name='watchtower.alert',
      version='0.1',
      description='Alert consumer component of the Watchtower framework',
      url='https://github.com/CAIDA/watchtower-alert',
      author='Alistair King',
      author_email='alistair@caida.org',
      license='BSD',
      packages=find_packages(),
      entry_points={'console_scripts': [
          'watchtower-alert-consumer=watchtower.alert.consumer:main'
      ]},
      install_requires=['confluent-kafka', 'sqlalchemy', 'psycopg2',
                        'requests', 'pytimeseries'])
