"""Setup file for biodiversity-data-ingestion package."""

import setuptools

setuptools.setup(
    name='biodiversity-data-ingestion',
    version='1.0.0',
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    install_requires=[
        'apache-beam[gcp]==2.62.0',
        'lxml',
        'elasticsearch==8.15.1',
        'google-cloud-secret-manager',
    ],
)