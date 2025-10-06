#!/usr/bin/env python
"""
Fallback setup script for dataflow_common.

This script delegates to setuptools in case the build backend defined in
pyproject.toml cannot be used directly by the environment (for
example, older versions of pip).  It simply reads the configuration
from setup.cfg.
"""
from setuptools import setup

if __name__ == "__main__":
    setup()