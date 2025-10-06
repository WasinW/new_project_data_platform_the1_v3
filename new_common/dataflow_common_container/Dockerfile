#
# Custom Dataflow worker image containing the dataflow_common library
# and its dependencies.  Modify the base image tag to match your
# Apache Beam version and Python version.
#
FROM apache/beam_python3.11_sdk:2.59.0

# Copy the library source code into the image
COPY src/dataflow_common /opt/dataflow_common

# Copy package metadata (optional, in case you want to build wheel inside)
COPY pyproject.toml setup.cfg setup.py /opt/dataflow_common/

# Copy dependency list
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies (avoid installing apache-beam here)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Install the dataflow_common library from source
RUN pip install --no-cache-dir /opt/dataflow_common

# Expose a PYTHONPATH so Python can locate the package
ENV PYTHONPATH="/opt:${PYTHONPATH}"

# The default entrypoint is provided by the Beam SDK harness.  No CMD is needed.
