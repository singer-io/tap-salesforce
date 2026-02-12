#!/usr/bin/env python

from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='tap-perplexity',
    version='1.0.0',
    description='Singer.io tap for extracting data from the Perplexity AI API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Singer.io',
    url='https://github.com/singer-io/tap-perplexity',
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    py_modules=['tap_perplexity'],
    install_requires=[
        'requests==2.32.5',
        'singer-python==6.3.0',
        'backoff==2.2.1',
    ],
    extras_require={
        'dev': [
            'pytest==7.4.3',
            'pytest-cov==4.1.0',
            'pylint==3.0.3',
            'black==23.12.1',
        ]
    },
    entry_points='''
        [console_scripts]
        tap-perplexity=tap_perplexity:main
    ''',
    packages=find_packages(exclude=['tests']),
    package_data={
        'tap_perplexity': ['schemas/*.json']
    },
    include_package_data=True,
)
