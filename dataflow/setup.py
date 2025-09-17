# dataflow/setup.py (for Dataflow workers)
"""Setup file for Dataflow pipeline dependencies"""

import setuptools

setuptools.setup(
    name='ms_member_pipeline',
    version='1.0.0',
    install_requires=[
        # Install from Artifact Registry
        'dataflow-common-the1>=1.0.0',
        'apache-beam[gcp]==2.50.0',
    ],
    dependency_links=[
        # Point to your Artifact Registry
        # 'https://asia-southeast1-python.pkg.dev/120574803/python-packages/simple/'
        'https://asia-southeast1-python.pkg.dev/the1-insight-dev/python-packages/simple/'
    ],
    packages=setuptools.find_packages(),
)