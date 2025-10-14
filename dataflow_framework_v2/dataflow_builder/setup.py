from setuptools import setup, find_packages

setup(
    name="dataflow-builder",
    version="2.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "apache-beam>=2.59.0",
        "pyyaml>=6.0",
    ],
    description="Dataflow pipeline builder (driver side)",
)
