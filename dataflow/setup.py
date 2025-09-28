# dataflow/FW/setup.py
import setuptools

setuptools.setup(
    name='unified_pipeline',
    version='1.0.0',
    py_modules=['unified_dataflow_pipeline_bigtable'],
    install_requires=[
        'apache-beam[gcp]==2.59.0',
        'google-cloud-bigquery==3.25.0',
        'google-cloud-bigtable==2.23.0',
    ]
)