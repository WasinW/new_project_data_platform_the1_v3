# ใช้ Beam SDK version ที่ตรงกับที่คุณใช้
FROM apache/beam_python3.11_sdk:2.59.0

# ติดตั้ง dependencies ทั้งหมดที่ต้องการ
RUN pip install --no-cache-dir \
    apache-beam[gcp]==2.59.0 \
    apache-beam[aws]==2.59.0 \
    google-cloud-bigquery==3.25.0 \
    boto3>=1.28.0 \
    botocore>=1.31.0 \
    s3transfer>=0.6.0 \
    pyarrow>=12.0.0

# ตรวจสอบว่า boto3 พร้อมใช้งาน
RUN python -c "from apache_beam.io.aws.clients.s3 import boto3_client; print('✓ boto3 available')"

# สำคัญมาก: ใช้ global Python environment
ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

# Optional: ถ้าต้องการ copy scripts เข้าไปใน image ด้วย
# COPY test_bq_read_simple4.py /app/
# WORKDIR /app