#
# Custom Dataflow worker image containing the dataflow_common library
# and its dependencies.  Modify the base image tag to match your
# Apache Beam version and Python version.
#
# FROM apache/beam_python3.11_sdk:2.59.0

# # Copy only the library source code into the image.  We avoid installing
# # the package via setuptools because the setup configuration expects
# # a ``src`` directory at the project root.  By copying the package
# # folder directly and adjusting PYTHONPATH, we make the code importable
# # without running ``pip install``.
# #
# # The destination is ``/opt/dataflow_common`` which will contain the
# # ``dataflow_common`` package as a subfolder.  Setting PYTHONPATH
# # accordingly allows Python to find ``dataflow_common``.
# # COPY src/dataflow_common /opt/dataflow_common
# COPY src/dataflow_common /usr/local/lib/python3.11/site-packages/dataflow_common

# # Copy dependency list and install dependencies.  Do not install
# # ``apache-beam`` here because it is already provided by the base
# # image.
# COPY requirements.txt /tmp/requirements.txt
# RUN pip install --no-cache-dir --upgrade pip && \
#     pip install --no-cache-dir -r /tmp/requirements.txt

# # ------------------------------------------------------------------------------
# FROM apache/beam_python3.11_sdk:2.59.0

# # Avoid pip going online later and keep images reproducible
# RUN pip install --no-cache-dir --upgrade pip \
#  && pip install --no-cache-dir --no-deps six==1.16.0 python-dateutil==2.9.0.post0 jmespath==1.0.1 urllib3==1.26.18 \
#  && pip install --no-cache-dir --no-deps botocore==1.34.106 s3transfer==0.10.1 \
#  && pip install --no-cache-dir --no-deps boto3==1.34.106 \
#  && pip install --no-cache-dir pyarrow==14.0.2 google-cloud-bigquery==3.25.0

# # (ถ้าคุณ build dataflow_common เป็น wheel ไว้แล้ว)
# # COPY dist/dataflow_common-*.whl /tmp/
# # RUN pip install --no-cache-dir /tmp/dataflow_common-*.whl
# # Create a temporary directory for package installation
# WORKDIR /tmp/dataflow_common_install

# # ------------------------------------------------------------------------------
# # # หรือถ้าเป็นซอร์สโค้ด
# # COPY src/dataflow_common /opt/dataflow_common
# # RUN pip install --no-cache-dir /opt/dataflow_common
# # ------------------------------------------------------------------------------
# # Copy the entire package structure with setup files
# COPY pyproject.toml setup.cfg setup.py ./
# COPY src ./src/

# # Install the package globally (this will install to site-packages)
# RUN pip install --no-cache-dir .
# # ------------------------------------------------------------------------------
# # ENV PYTHONPATH=/root/.local/lib/python3.11/site-packages:${PYTHONPATH}
# # Clean up and reset working directory
# WORKDIR /
# RUN rm -rf /tmp/dataflow_common_install


# ----------------------------------------------------------------------------
# FROM apache/beam_python3.11_sdk:2.59.0

# # Install dependencies first
# RUN pip install --no-cache-dir --upgrade pip \
#  && pip install --no-cache-dir --no-deps six==1.16.0 python-dateutil==2.9.0.post0 jmespath==1.0.1 urllib3==1.26.18 \
#  && pip install --no-cache-dir --no-deps botocore==1.34.106 s3transfer==0.10.1 \
#  && pip install --no-cache-dir --no-deps boto3==1.34.106 \
#  && pip install --no-cache-dir pyarrow==14.0.2 google-cloud-bigquery==3.25.0 \
#  && pip install --no-cache-dir pyyaml==6.0.1

# # Copy the source code directly to Python's site-packages
# # This is the most reliable way for the Beam SDK container
# COPY src/dataflow_common /usr/local/lib/python3.11/site-packages/dataflow_common

# # Verify the installation
# RUN python -c "import dataflow_common; print('dataflow_common imported successfully')"

# -------------------------------------------------------------------------------
# # flex_template.Dockerfile
# FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# # Install dependencies for launcher
# COPY requirements.txt /template/
# RUN pip install --no-cache-dir -r /template/requirements.txt

# # Copy and install dataflow_common
# COPY src/dataflow_common /template/dataflow_common/
# ENV PYTHONPATH=/template:$PYTHONPATH

# # Copy pipeline code
# COPY ms_member_short_pipeline.py /template/

# ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/ms_member_short_pipeline.py"
# ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

# # Use custom SDK container for workers
# ENV DATAFLOW_PYTHON_SDK_CONTAINER_IMAGE="asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-common:v1.6"
# -------------------------------------------------------------------------------
FROM apache/beam_python3.11_sdk:2.59.0

# ป้องกันการสร้าง venv ใหม่
ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir boto3==1.34.106 \
    && pip install --no-cache-dir pyarrow==14.0.2 \
    && pip install --no-cache-dir google-cloud-bigquery==3.25.0

# Copy source code ตรงๆ ไปที่ site-packages
COPY src/dataflow_common /usr/local/lib/python3.11/site-packages/dataflow_common

# Verify installation
RUN python -c "from dataflow_common.config import load_config; print('Import successful!')" \
    && python -c "import dataflow_common; print('Version:', dataflow_common.__version__)"

WORKDIR /