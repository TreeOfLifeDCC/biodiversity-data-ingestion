"""Setup file for the biodiversity-data-ingestion Beam package."""

from setuptools import find_packages, setup


setup(
    name="biodiversity-data-ingestion",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "apache-beam[gcp]==2.62.0",
        "lxml",
        "elasticsearch==8.17.1",
        "google-cloud-secret-manager",
    ],
)
