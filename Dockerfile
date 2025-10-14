FROM apache/beam_python3.11_sdk:2.59.0

ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

# Install build dependencies
RUN pip install --upgrade pip setuptools wheel

# Copy wheel files
COPY dataflow_framework_v2/dataflow_builder/dist/*.whl /tmp/
COPY dataflow_framework_v2/dataflow_worker/dist/*.whl /tmp/

# Install dataflow packages
RUN pip install /tmp/dataflow_builder-*.whl
RUN pip install /tmp/dataflow_worker-*.whl

# Install additional dependencies
RUN pip install --no-cache-dir \
    boto3==1.34.106 \
    pyarrow==14.0.2 \
    google-cloud-bigquery==3.25.0 \
    pyyaml==6.0.1

# Clean up
RUN rm -rf /tmp/*.whl

# Verify installation
RUN python -c "from dataflow_builder.config import PipelineConfig; print('dataflow-builder OK')"
RUN python -c "from dataflow_worker.steps import ReadBQQueryStep; print('dataflow-worker OK')"

WORKDIR /
