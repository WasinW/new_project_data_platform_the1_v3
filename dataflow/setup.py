# ไฟล์: setup.py  (วางไว้ที่ root ของ repo)
import setuptools

setuptools.setup(
    name='unified_dataflow_pipeline',
    version='1.0.0',  # ปรับ version ตามต้องการ
    packages=['dataflow_common'],       # ติดตั้ง package dataflow_common ทั้งโฟลเดอร์
    py_modules=['unified_dataflow_pipeline_bigtable'],  # รวมสคริปต์ pipeline เป็น module
    install_requires=[
        'google-cloud-bigquery==3.25.0',
        'google-cloud-bigtable==2.23.0',
        # ไม่ต้องใส่ apache-beam เพราะ Dataflow จะจัดการเอง
    ],
)
