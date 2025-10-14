from setuptools import setup, find_packages

setup(
    name="dataflow-worker",
    version="2.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "dataflow-builder>=2.0.0",  # For shared config
        "apache-beam>=2.59.0",
        "google-cloud-bigquery>=3.25.0",
        "pyarrow>=14.0.0",
        "boto3>=1.34.0",
        "pyyaml>=6.0",
    ],
    description="Dataflow pipeline worker (execution side)",
)
