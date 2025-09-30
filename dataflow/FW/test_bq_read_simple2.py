#!/usr/bin/env python
"""
Simple BigQuery Read Test - ทดสอบอ่านข้อมูลจาก BigQuery
รองรับทั้ง DirectRunner และ DataflowRunner
"""

import re
import json
import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions, SetupOptions
# from apache_beam.io import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery

# Setup logging
logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# ---------------------------
# 1) Helper: สร้าง mapping_dict จาก mapping_list (ตามที่คุณระบุไว้)
# ---------------------------
def create_mapping_dict(mapping_list):
    """
    mapping_list: list ของ row dicts จากตาราง mapping
      - RECONCILE_COLUMN_NAME: ชื่อคีย์ใหม่ (target key)
      - PERSONAS_MAPPING_COLUMN_NAME: เส้นทาง (dot-path) ไปยังค่าจาก source
    """
    mapping_dict = {}
    for item in mapping_list:
        if not item:
            continue
        key = item.get('RECONCILE_COLUMN_NAME')
        # reconcile_column = item.get('PERSONAS_MAPPING_COLUMN_NAME').split('.')[-1] if item.get('RECONCILE_RETRIEVED') else None
        # original_column = item.get('PERSONAS_MAPPING_COLUMN_NAME').split('.')[-1] if item.get('RECONCILE_CONFIRMED') else None
        # mapping_dict[key] = {}
        # mapping_dict[key]["reconcile"] = reconcile_column
        # mapping_dict[key]["original"] = original_column
        mapping_dict[key] = item
    return mapping_dict

# ---------------------------
# 3) ตัวแปลงหลัก: ใช้ mapping_dict ทำ rename + lookup ค่า
# ---------------------------
def apply_mapping(record, mapping_dict, keep_none=False):
    """
    record: แถว/ข้อความต้นทาง (dict)
    mapping_dict: {new_key: src_path (dot-path)}
    keep_none: ถ้า True จะเก็บ key ที่หาไม่พบเป็น None ด้วย
    """
    out_reconcile = {}
    # out_original = {}
    for org_col, new_col in mapping_dict.items():
        out_reconcile[org_col] = record.get(new_col[org_col]["RECONCILE_COLUMN_NAME"]) if new_col[org_col]["RECONCILE_RETRIEVED"] else None
        # out_original[org_col] = record.get(new_col["original"])
    # return out_reconcile, out_original
    return out_reconcile


def log_count(count):
    """Helper function to log count results"""
    logging.info(f"✅ SUCCESS: Got {count} records from BigQuery")
    return count

def to_kv_new(d):
    # d = new_message (ยังไม่ fallback)
    return (d.get('profiles').get('memberId'), d)

def to_kv_origin(r):
    # r = origin row
    return (r.get('member_number'), r)
# def coalesce_per_column(kv, columns):
#     key, groups = kv
#     news = groups['new']   # list
#     olds = groups['old']   # list
#     new_row = news[0] if news else {}
#     old_row = olds[0] if olds else {}
#     out = {}
#     for c in columns:
#         v = new_row.get(c)
#         out[c] = v if v is not None else old_row.get(c)
#     return out
def coalesce_per_column(kv, columns, condition_col):
    key, groups = kv
    news = groups['new']   # list
    olds = groups['old']   # list
    new_row = news[0] if news else {}
    old_row = olds[0] if olds else {}
    out = {}
    for c in columns:
        v = new_row.get(c) if c.get(condition_col) else old_row.get(c)
        out[c] = v
    return out


def run():
    """Run the pipeline with proper project handling"""
    
    # Parse all arguments
    pipeline_options = PipelineOptions()
    
    # Get the Google Cloud options view
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    
    # The project should already be set via --project parameter from DAG
    project = google_cloud_options.project
    
    if not project:
        raise ValueError("Project must be specified via --project parameter")
    
    logging.info(f"Using project: {project}")
    logging.info(f"Pipeline options: {pipeline_options.get_all_options()}")
    
    # Build the query
    query = f"""
    SELECT *
    FROM `{project}.insight_dev.personas_test`
    LIMIT 100
    """
    source_query = f"""
        SELECT * EXCEPT(RN_PK)
        FROM (
            SELECT *
            , ROW_NUMBER() OVER(PARTITION BY JSON_VALUE(profiles, '$.memberId') ORDER BY TIMESTAMP DESC) RN_PK
            FROM `{project}.insight_dev.personas_test`
        ) AS LAST_UPD
        WHERE RN_PK = 1 
    """

    logging.info(f"Query: {source_query}")
    mapping_query = f"""
        SELECT RECONCILE_COLUMN_NAME,
            PERSONAS_MAPPING_COLUMN_NAME,
            RECONCILE_RETRIEVED,
            RECONCILE_CONFIRMED,
            UPDATED_DATE
        FROM `{project}.insight_dev.stg_mapping_reconcile`
        WHERE COALESCE(UPDATED_DATE, '1999-12-31') = (
            SELECT COALESCE(MAX(UPDATED_DATE), '1999-12-31')
            FROM `{project}.insight_dev.stg_mapping_reconcile`
        )
    """
    logging.info(f"Query: {mapping_query}")

    origin_query = f"""SELECT * FROM `{project}.insight_dev.stg_ms_member`"""
    logging.info(f"Query: {origin_query}")

    # Run the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        mapping_data = (
            p
            | 'ReadFromBigQuery_MAPPING' >> ReadFromBigQuery(
                query=mapping_query,
                use_standard_sql=True,
                project=project,
                gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
            )
        )
        source_data = (
            p
            | 'ReadFromBigQuery_SRC_DATA' >> ReadFromBigQuery(
                query=source_query,
                use_standard_sql=True,
                project=project,
                gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
                # gcs_location=config.get('gcs_location') or config.get('temp_location')
            )
        )
        count_src = (
            source_data
            | 'Count_SRC_DATA' >> beam.combiners.Count.Globally()
            | 'LogResults_SRC_DATA' >> beam.Map(log_count)  # Use function instead of lambda
        )

        origin_data = (
            p
            | 'ReadFromBigQuery_ORIGIN_DATA' >> ReadFromBigQuery(
                query=origin_query,
                use_standard_sql=True,
                project=project,
                gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
                # gcs_location=config.get('gcs_location') or config.get('temp_location')
            )
        )
        count_org = (
            origin_data
            | 'Count_ORG_DATA' >> beam.combiners.Count.Globally()
            | 'LogResults_ORG_DATA' >> beam.Map(log_count)  # Use function instead of lambda
        )

        # กันเคส mapping ว่าง ให้ได้ dict ว่างหนึ่งตัว (เพื่อใช้ AsSingleton ได้)
        mapping_with_default = (
            mapping_data
            | 'EnsureNotEmpty' >> beam.FlatMap(lambda x: [x] if x else [{}])
        )

        # สร้าง side input: mapping_dict เพียงตัวเดียว (efficient กว่า build ซ้ำทุก element)
        mapping_dict_pc = (
            mapping_with_default
            | 'ToList' >> beam.combiners.ToList()
            | 'BuildMappingDict' >> beam.Map(create_mapping_dict)
        )
        mapping_dict_side = beam.pvalue.AsSingleton(mapping_dict_pc)
        # mapping_dict_side = beam.pvalue.AsSingleton(mapping_with_default)

        # ========== APPLY MAPPING ==========
        # ได้ PCollection ของ new_message ตาม mapping แล้ว
        # new_messages = (
        #     source_data
        #     | 'ApplyMapping' >> beam.Map(apply_mapping, mapping_dict=mapping_dict_side, keep_none=False)
        # )

        # ========== JOIN & COALESCE ==========
        # 1) map -> new_message ตาม mapping_dict ก่อน (ไม่ต้องมี origin)
        mapped_new = source_data | 'MapOnly_new_source' >> beam.Map(apply_mapping, mapping_dict=mapping_dict_side)

        # (apply_mapping_only = เวอร์ชันที่ใช้ mapping_dict สร้าง new_message แต่ยังไม่ fallback)
        # map_origin = origin_data | 'MapOnly_origin' >> beam.Map(apply_mapping, mapping_dict=mapping_dict_side)
        map_origin = origin_data 

        # 2) join
        joined = ({
            'new': mapped_new        | 'KV_new'    >> beam.Map(to_kv_new),
            'old': map_origin       | 'KV_origin' >> beam.Map(to_kv_origin),
        }) | 'Join' >> beam.CoGroupByKey()

        # 3) coalesce ต่อฟิลด์ที่ต้องการ (columns = list(RECONCILE_COLUMN_NAME))
        # columns = beam.pvalue.AsSingleton(
        #     mapping_with_default | 'CollectColumns' >> beam.Map(lambda r: r.get('RECONCILE_COLUMN_NAME')) | beam.combiners.ToList()
        # )
        # final_rows = joined | 'CoalesceFinal_rows' >> beam.Map(coalesce_per_column, columns=columns)
        patch_origin_rows = joined | 'CoalesceFinal_rows' >> beam.Map(coalesce_per_column, columns=mapping_with_default,condition_col="RECONCILE_CONFIRMED")
        reconcile_rows = joined | 'CoalesceFinal_rows' >> beam.Map(coalesce_per_column, columns=mapping_with_default,condition_col="RECONCILE_RETRIEVED")
        # result_rows = joined | 'CoalesceFinal_rows' >> beam.Map(coalesce_per_column, columns=mapping_with_default)

        count_patch_origin_rows = (
            patch_origin_rows
            | 'Count_MS_MEMBER' >> beam.combiners.Count.Globally()
            | 'LogResults_MS_MEMBER' >> beam.Map(log_count)  # Use function instead of lambda
        )

        count_reconcile_rows = (
            reconcile_rows
            | 'Count_MS_PERSONAS' >> beam.combiners.Count.Globally()
            | 'LogResults_MS_PERSONAS' >> beam.Map(log_count)  # Use function instead of lambda
        )

    logging.info("Pipeline completed successfully")

if __name__ == '__main__':
    # run_simple_test()
    logging.getLogger().setLevel(logging.INFO)
    run()
