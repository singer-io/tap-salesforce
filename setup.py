#!/usr/bin/env python

from setuptools import setup

setup(name='tap-salesforce',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Salesforce API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_salesforce'],
      install_requires=[
          'simple-salesforce==0.73.0',
          'requests==2.12.4',
          'singer-python==3.6.0',
          'xmltodict==0.11.0',
      ],
      entry_points='''
          [console_scripts]
          tap-salesforce=tap_salesforce:main
      ''',
      packages=['tap_salesforce'],
      package_data = {
          'tap_salesforce/schemas': [
              # add schema.json filenames here
          ]
      },
      include_package_data=True,
)
