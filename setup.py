#!/usr/bin/env python

from setuptools import setup

setup(name='tap-salesforce',
      version='1.1.1',
      description='Singer.io tap for extracting data from the Salesforce API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_salesforce'],
      install_requires=[
          'pybase62==0.3.2',
          'requests==2.12.4',
          'singer-python==5.0.0',
          'xmltodict==0.11.0',
          'nose'
      ],
      entry_points='''
          [console_scripts]
          tap-salesforce=tap_salesforce:main
      ''',
      packages=['tap_salesforce', 'tap_salesforce.salesforce'],
      package_data = {
          'tap_salesforce/schemas': [
              # add schema.json filenames here
          ]
      },
      include_package_data=True,
)
