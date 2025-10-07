# Base image for dataflow_common library.
#
# This image copies the generic `dataflow_common` package into the
# container and installs its dependencies.  It is intended to be used
# as a build stage for pipeline images rather than being run directly.

FROM apache/beam_python3.11_sdk:2.59.0

# Copy the library source code into the image
COPY src/dataflow_common /opt/dataflow_common

# Copy and install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# No entrypoint set.  This image is used for copying library code into
# downstream pipeline images.