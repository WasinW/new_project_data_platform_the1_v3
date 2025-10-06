#
# Custom Dataflow worker image containing the dataflow_common library
# and its dependencies.  Modify the base image tag to match your
# Apache Beam version and Python version.
#
FROM apache/beam_python3.11_sdk:2.59.0

# Copy only the library source code into the image.  We avoid installing
# the package via setuptools because the setup configuration expects
# a ``src`` directory at the project root.  By copying the package
# folder directly and adjusting PYTHONPATH, we make the code importable
# without running ``pip install``.
#
# The destination is ``/opt/dataflow_common`` which will contain the
# ``dataflow_common`` package as a subfolder.  Setting PYTHONPATH
# accordingly allows Python to find ``dataflow_common``.
COPY src/dataflow_common /opt/dataflow_common

# Copy dependency list and install dependencies.  Do not install
# ``apache-beam`` here because it is already provided by the base
# image.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Set PYTHONPATH so that the interpreter can locate the package.  We
# prefix /opt/dataflow_common because that directory contains the
# ``dataflow_common`` module directly.  We also include the existing
# PYTHONPATH, if any.
ENV PYTHONPATH="/opt/dataflow_common:${PYTHONPATH}"

# No further commands are necessary; the Beam SDK container provides
# the required entrypoint for the worker harness.
