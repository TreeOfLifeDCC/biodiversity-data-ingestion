"""Setup file for biodiversity-data-ingestion package."""

from setuptools import setup, find_packages

setup(
    name='aegis-etl',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    package_data={
        'dependencies': ['elasticsearch_settings/*.json'],
    },
    include_package_data=True,
    install_requires=[
        'apache-beam[gcp]==2.62.0',
        'lxml',
        'elasticsearch==8.15.1',
        'google-cloud-secret-manager',
    ],
)