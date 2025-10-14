FROM apache/beam_python3.11_sdk:2.59.0

# Copy wheels
COPY dataflow_framework_v2/dataflow_builder/dist/*.whl /tmp/
COPY dataflow_framework_v2/dataflow_worker/dist/*.whl /tmp/

# Install packages
RUN pip install /tmp/dataflow-builder-*.whl && \
    pip install /tmp/dataflow-worker-*.whl && \
    rm /tmp/*.whl

# Install additional dependencies
RUN pip install --no-cache-dir \
    boto3==1.34.106 \
    pyarrow==14.0.2 \
    google-cloud-bigquery==3.25.0

WORKDIR /
