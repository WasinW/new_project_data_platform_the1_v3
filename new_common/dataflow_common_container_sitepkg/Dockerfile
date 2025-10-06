#
# Custom Dataflow worker image containing the dataflow_common library
# and its dependencies.  Modify the base image tag to match your
# Apache Beam version and Python version.
#
FROM apache/beam_python3.11_sdk:2.59.0

# Copy the library source code into the Python site‑packages directory.  By
# copying into site‑packages we make ``dataflow_common`` importable without
# relying on $PYTHONPATH propagation.  The target path matches the Python
# 3.11 site‑packages location used in the Beam SDK base image.  If you
# upgrade the Python version in the base image, adjust this path accordingly.
COPY src/dataflow_common /usr/local/lib/python3.11/site-packages/dataflow_common

# Install additional Python dependencies.  We deliberately avoid installing
# ``apache‑beam`` here because it is provided by the base image.  The
# dependencies listed in ``requirements.txt`` should include only the
# libraries that your pipelines depend on (e.g. boto3, pyarrow, etc.).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# No further commands are necessary; the Beam SDK container provides the
# required entrypoint for the worker harness.
