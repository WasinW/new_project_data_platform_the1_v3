from setuptools import setup, find_packages

setup(
    name="dataflow_common",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyyaml>=6.0",
        "apache-beam>=2.59.0",
        "google-cloud-bigquery>=3.25.0",
        "pyarrow>=14.0.0",
    ],
)