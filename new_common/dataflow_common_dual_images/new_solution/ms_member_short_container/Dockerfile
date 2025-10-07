# Dockerfile for the `ms_member_short` pipeline image.
#
# This image packages the pipeline script and the reusable `dataflow_common`
# library into the same directory so that Google Cloud Dataflow will stage
# both together when executing the pipeline.  The image also installs
# dependencies from `requirements.txt` so that the worker does not need
# internet access at runtime.  It uses a multi‑stage build: the first stage
# copies the library and its dependencies; the final stage copies the
# library and pipeline script into `/app`.

## Stage 1: build common library and dependencies
FROM apache/beam_python3.11_sdk:2.59.0 as common

COPY dataflow_common /opt/dataflow_common
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

## Stage 2: final pipeline image
FROM apache/beam_python3.11_sdk:2.59.0

# Copy the library from the common stage into the application directory
COPY --from=common /opt/dataflow_common /app/dataflow_common

# Copy the pipeline script into the application directory
COPY ms_member_short_pipeline.py /app/ms_member_short_pipeline.py

# Copy the requirements file for completeness and install dependencies.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Set the working directory so relative imports resolve correctly.  When
# Dataflow stages the pipeline, it will include this directory and its
# contents.
WORKDIR /app

# No entrypoint; Dataflow will invoke the pipeline using the py_file flag.