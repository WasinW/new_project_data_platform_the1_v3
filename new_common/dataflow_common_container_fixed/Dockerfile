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

# ------------------------------------------------------------------------------
FROM apache/beam_python3.11_sdk:2.59.0

# Avoid pip going online later and keep images reproducible
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir --no-deps six==1.16.0 python-dateutil==2.9.0.post0 jmespath==1.0.1 urllib3==1.26.18 \
 && pip install --no-cache-dir --no-deps botocore==1.34.106 s3transfer==0.10.1 \
 && pip install --no-cache-dir --no-deps boto3==1.34.106 \
 && pip install --no-cache-dir pyarrow==14.0.2 google-cloud-bigquery==3.25.0

# (ถ้าคุณ build dataflow_common เป็น wheel ไว้แล้ว)
# COPY dist/dataflow_common-*.whl /tmp/
# RUN pip install --no-cache-dir /tmp/dataflow_common-*.whl
# Create a temporary directory for package installation
WORKDIR /tmp/dataflow_common_install

# ------------------------------------------------------------------------------
# # หรือถ้าเป็นซอร์สโค้ด
# COPY src/dataflow_common /opt/dataflow_common
# RUN pip install --no-cache-dir /opt/dataflow_common
# ------------------------------------------------------------------------------
# Copy the entire package structure with setup files
COPY pyproject.toml setup.cfg setup.py ./
COPY src ./src/

# Install the package globally (this will install to site-packages)
RUN pip install --no-cache-dir .
# ------------------------------------------------------------------------------
# ENV PYTHONPATH=/root/.local/lib/python3.11/site-packages:${PYTHONPATH}
# Clean up and reset working directory
WORKDIR /
RUN rm -rf /tmp/dataflow_common_install
