# dataflow/setup.py
import setuptools

setuptools.setup(
    name='unified_pipeline',
    version='1.0.0',
    install_requires=[
        'apache-beam[gcp]==2.59.0',
        'google-cloud-bigquery==3.25.0',
        'google-cloud-bigtable==2.23.0',
    ],
    packages=['dataflow_common'],  # Include local package
    package_dir={'dataflow_common': 'packages/dataflow-common/dataflow_common'}
)