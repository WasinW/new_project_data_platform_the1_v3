# dataflow/FW/setup.py
import subprocess
import sys
from setuptools import setup, find_packages
from setuptools.command.install import install

class CustomInstallCommand(install):
    """Custom installation to install wheel packages"""
    def run(self):
        # Install the wheel package directly
        subprocess.check_call([
            sys.executable, '-m', 'pip', 'install',
            'gs://t1-dataflow-framework-bucket/common/packages/dataflow_common-1.0.0-py3-none-any.whl'
        ])
        install.run(self)

setup(
    name='unified_pipeline',
    version='1.0.0',
    py_modules=['unified_dataflow_pipeline_bigtable'],
    cmdclass={
        'install': CustomInstallCommand,
    },
    install_requires=[
        'apache-beam[gcp]==2.59.0',
        'google-cloud-bigquery==3.25.0',
        'google-cloud-bigtable==2.23.0',
    ]
)