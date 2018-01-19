#!/usr/bin/env python3
from setuptools import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()


setup(
    name='awslogs-sd',
    version='0.1',
    description="Forward systemd journal logs to cloudwatch",
    long_description=readme,
    author="Marcin Bachry",
    author_email='hegel666@gmail.com',
    url='https://github.com/mbachry/awslogs-sd',
    packages=['awslogs_sd'],
    install_requires=[
        'awscli-cwlogs',
        'boto3',
        'gevent',
        'pyyaml',
        'requests',
        'retrying',
        'systemd-python',
    ],
    entry_points={
        'console_scripts': [
            'awslogs-sd=awslogs_sd.awslogs_sd:main'
        ],
    },
    license="MIT",
    zip_safe=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
