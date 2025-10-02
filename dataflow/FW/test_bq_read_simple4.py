#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dataflow / Beam: Read BQ -> map by dynamic mapping -> left-join origin -> per-column coalesce
- Fixes NameError: extract_by_path not defined
- Fixes incorrect mapping_dict access & join key
- Uses side inputs correctly
"""

import json
import logging
import re
from typing import Any, Dict, List, Tuple
import os
from datetime import datetime, timezone , date

import pyarrow as pa
from apache_beam.io.parquetio import WriteToParquet

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# -------- Config (เปลี่ยนได้ตามจริง) --------
PK_NAME = "member_number"  # primary key หลัง mapping
TEMP_GCS = "gs://t1-insight-audit-bucket/audit_log/dataflow/temp"

# สร้าง run_dt ในรูปแบบ YYYYMMDDHH
def get_run_dt() -> str:
    """สร้าง run_dt จากเวลาปัจจุบัน เช่น 2025100209"""
    now = datetime.now()
    return now.strftime("%Y%m%d%H")

RUN_DT_STR = get_run_dt()  # เรียกครั้งเดียวตอนเริ่มต้น

# ---------------------------
# Utilities for mapping paths
# ---------------------------
_JSON_VALUE_RE = re.compile(r"""(?ix)
    JSON_VALUE\(
        \s*`?([\w.]+)`?\s* ,    # column name (group 1)
        \s*'([^']*)'\s*         # json-path, like $.memberId (group 2)
    \)
""")

def normalize_path(path: str) -> str:
    """Normalize mapping path to a simple dot-path, e.g.
    - "JSON_VALUE(profiles, '$.memberId')" -> "profiles.memberId"
    - "$.profile.id" -> "profile.id"
    - "profiles.memberId" -> unchanged
    """
    if not path:
        return ""
    s = path.strip().strip("`")
    m = _JSON_VALUE_RE.fullmatch(s)
    if m:
        col = m.group(1).strip(" `")
        jp = m.group(2).strip()
        if jp.startswith("$."):
            jp = jp[2:]
        return f"{col}.{jp}" if jp else col

    if s.startswith("$."):
        return s[2:]
    return s

def extract_by_path(record: Any, path: str) -> Any:
    """Traverse dict/list (and JSON-in-string) by simple dot path.
    Supports array indexes if a path part is a number (e.g. 'phones.0.number').
    """
    if not path:
        return None
    target = record
    parts = [p for p in path.split(".") if p != ""]
    for part in parts:
        if target is None:
            return None

        # กรณีเป็น JSON string
        if isinstance(target, str):
            try:
                target = json.loads(target)
            except Exception:
                return None

        if isinstance(target, dict):
            if part in target:
                target = target[part]
                continue
            # เผื่อคีย์ต่าง case
            lowered = part.lower()
            for k in list(target.keys()):
                if isinstance(k, str) and k.lower() == lowered:
                    target = target[k]
                    break
            else:
                return None
        elif isinstance(target, list):
            if part.isdigit():
                idx = int(part)
                if 0 <= idx < len(target):
                    target = target[idx]
                else:
                    return None
            else:
                return None
        else:
            return None
    return target

def normalize_row_to_schema(row: dict, schema: pa.Schema) -> dict:
    """
    - เติมคีย์ที่หายให้ครบ
    - แปลงชนิดตาม schema:
      * string -> str หรือ ""
      * date32 -> datetime.date
      * timestamp(us) -> datetime.datetime (naive)
    - ค่าว่าง/"" -> None สำหรับ date/timestamp
    """
    out = {}
    for name, field in zip(schema.names, schema):
        v = row.get(name)
        if pa.types.is_string(field.type):
            out[name] = "" if v is None else str(v)
        elif pa.types.is_date(field.type):
            vv = None if v in (None, "") else parse_date_safe(v)
            out[name] = vv
        elif pa.types.is_timestamp(field.type):
            vv = None if v in (None, "") else parse_ts_safe(v)
            out[name] = vv
        else:
            # โครงนี้ไม่มีชนิดอื่น แต่กันไว้
            out[name] = v
    return out

# คอลัมน์ชนิดพิเศษตามสคีมาที่คุณให้มา
DATE_COLS = {
    'passport_exp', 'birth_date', 'employee_join_date'
}
TS_COLS = {
    'register_date', 'employee_resign_date', 'created_date',
    'updated_date', 'consent_date', 'etl_created_tms'
}

# รายชื่อคอลัมน์ทั้งหมดตามตารางจริงของคุณ (เรียงตามต้องการ)
ALL_COLS = [
    'member_id','member_number','nationality','country','passport_exp','birth_date','age',
    'mobile_country_code','home_ph_country_code','type_of_housing','sub_district','district','city','postal_code',
    'member_type','status_code','hold_reason','register_date','member_ref_by_name','member_ref_by_id',
    'register_channel','register_partner','gender','marital_status','job_title','education','monthly_income',
    'prefer_lang','customer_type','register_staff','register_branch','privacy_flag','data_invalid_flag','dummy_flag',
    'employee_bu_group','employee_bu','employee_resign_date','employee_join_date','employee_id','created_date',
    'member_number_merged','register_partner_code','register_branch_code','is_address','is_mobile','is_email',
    'is_send_sms_eng','is_send_sms_thai','is_expate','is_cds_line','is_rbs_line','is_ssp_line','is_cpn_line',
    'is_cfr_line','is_cfm_line','is_twd_line','is_the1_line','updated_date','insurance_not_send','consent_flag',
    'consent_channel','consent_version','consent_date','iscall','is_call_cds','is_email_cds','is_address_cds',
    'is_send_sms_eng_cds','is_send_sms_thai_cds','is_call_rbs','is_email_rbs','is_address_rbs','is_send_sms_eng_rbs',
    'is_send_sms_thai_rbs','is_call_b2s','is_email_b2s','is_address_b2s','is_send_sms_eng_b2s','is_send_sms_thai_b2s',
    'is_call_hws','is_email_hws','is_address_hws','is_send_sms_eng_hws','is_send_sms_thai_hws','is_call_twd',
    'is_email_twd','is_address_twd','is_send_sms_eng_twd','is_send_sms_thai_twd','is_call_ssp','is_email_ssp',
    'is_address_ssp','is_send_sms_eng_ssp','is_send_sms_thai_ssp','is_call_pwb','is_email_pwb','is_address_pwb',
    'is_send_sms_eng_pwb','is_send_sms_thai_pwb','is_call_ofm','is_email_ofm','is_address_ofm','is_send_sms_eng_ofm',
    'is_send_sms_thai_ofm','is_call_cfm','is_email_cfm','is_address_cfm','is_send_sms_eng_cfm','is_send_sms_thai_cfm',
    'is_call_cfr','is_email_cfr','is_address_cfr','is_send_sms_eng_cfr','is_send_sms_thai_cfr','is_call_cmg',
    'is_email_cmg','is_address_cmg','is_send_sms_eng_cmg','is_send_sms_thai_cmg','th_title','eng_title',
    'ever_consent_partner','is_consent_the1','etl_created_by','etl_created_tms','invalid_member_flag','invalid_type'
]

def build_fixed_schema() -> pa.Schema:
    fields = []
    for c in ALL_COLS:
        if c in DATE_COLS:
            fields.append(pa.field(c, pa.date32()))
        elif c in TS_COLS:
            fields.append(pa.field(c, pa.timestamp('us')))  # timestamp w/o tz
        else:
            fields.append(pa.field(c, pa.string()))
    return pa.schema(fields)

PARQUET_SCHEMA = build_fixed_schema()
_TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
]
_DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]

def parse_date_safe(v):
    if v is None: return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s: return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # ติดกรณีพิเศษ/ค่าขยะ → None
    return None

def parse_ts_safe(v):
    if v is None: return None
    if isinstance(v, datetime):
        # ทำให้เป็น naive (ไม่มี tz) ถ้ามี tz มา
        return v.replace(tzinfo=None)
    s = str(v).strip()
    if not s: return None
    # ตัด 'Z' ท้าย (ถ้ามี) ให้เป็น naive
    if s.endswith('Z'):
        s = s[:-1]
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # เผื่อส่งมาเป็น epoch seconds/millis
    try:
        iv = int(s)
        # เดา: ถ้าตัวเลขยาว > 10 น่าจะ millis
        if len(s) > 10:
            return datetime.fromtimestamp(iv/1000.0)
        return datetime.fromtimestamp(iv)
    except Exception:
        return None

# ---------------------------
# Mapping builders
# ---------------------------
def create_mapping_dict(mapping_rows: List[dict]) -> Dict[str, dict]:
    """Create {target_col: {'src_path': str, 'reconcile': bool, 'original': bool}}"""
    mapping: Dict[str, dict] = {}
    for row in mapping_rows:
        if not row:
            continue
        tgt = row.get("RECONCILE_COLUMN_NAME")
        src = row.get("PERSONAS_MAPPING_COLUMN_NAME")
        if not tgt:
            continue
        mapping[tgt] = {
            "src_path": normalize_path(src) if src else "",
            "reconcile": bool(row.get("RECONCILE_RETRIEVED")),
            "original": bool(row.get("RECONCILE_CONFIRMED")),
        }
    LOGGER.info("Built mapping for %d columns", len(mapping))
    return mapping

def map_record(record: dict, mapping_dict: Dict[str, dict], mode: str) -> dict:
    """คืน dict ที่มีคอลัมน์ปลายทางตาม mapping
    mode: 'reconcile' หรือ 'original' => ใช้แฟล็กที่ตรงกันตัดสินใจว่าจะดึงค่าจาก src หรือไม่
    """
    out: Dict[str, Any] = {}
    for tgt, cfg in mapping_dict.items():
        use_flag = cfg.get(mode, False)
        if not use_flag:
            continue
        src_path = cfg.get("src_path") or ""
        if not src_path:
            continue
        val = extract_by_path(record, src_path)
        out[tgt] = val
    return out

# ---------------------------
# KV helpers for join
# ---------------------------
def kv_from_new(d: dict) -> Tuple[Any, dict]:
    return (d.get(PK_NAME), d)

def kv_from_origin(r: dict) -> Tuple[Any, dict]:
    return (r.get(PK_NAME), r)

def key_is_not_none(kv: Tuple[Any, dict]) -> bool:
    return kv[0] is not None

# ---------------------------
# Coalesce helper
# ---------------------------
def coalesce_by_mapping(kv: Tuple[Any, dict], columns: List[dict], flag_field: str) -> dict:
    """Coalesce ต่อคอลัมน์ตามแฟล็กใน mapping:
    - ถ้า flag เป็น True => prefer new, else prefer old
    - ถ้า preferred เป็น None ให้ fallback ไปอีกฝั่ง
    - เติม PK ให้แน่ใจว่ามีใน output
    """
    _key, groups = kv
    news = groups.get("new") or []
    olds = groups.get("old") or []
    new_row = news[0] if news else {}
    old_row = olds[0] if olds else {}
    out: Dict[str, Any] = {}

    for row in columns:
        if not row:
            continue
        tgt = row.get("RECONCILE_COLUMN_NAME")
        if not tgt:
            continue
        prefer_new = bool(row.get(flag_field))
        preferred = (new_row.get(tgt) if prefer_new else old_row.get(tgt))
        if preferred is None:
            fallback = (old_row.get(tgt) if prefer_new else new_row.get(tgt))
            out[tgt] = fallback
        else:
            out[tgt] = preferred

    # ensure PK
    if PK_NAME in new_row:
        out.setdefault(PK_NAME, new_row.get(PK_NAME))
    if PK_NAME in old_row:
        out.setdefault(PK_NAME, old_row.get(PK_NAME))
    return out

# ---------------------------
# Beam pipeline
# ---------------------------
def run():
    pipeline_options = PipelineOptions()
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    setup_opts = pipeline_options.view_as(SetupOptions)
    setup_opts.save_main_session = True  # สำคัญมากเวลาไปรันบน Dataflow

    project = google_cloud_options.project
    if not project:
        raise ValueError("Project must be specified via --project")

    LOGGER.info("Using project: %s", project)
    LOGGER.info("Options: %s", pipeline_options.get_all_options())

    # BQ queries
    source_query = f"""
        SELECT * EXCEPT(RN_PK)
        FROM (
            SELECT *
                , ROW_NUMBER() OVER(
                    PARTITION BY JSON_VALUE(profiles, '$.memberId')
                    ORDER BY TIMESTAMP DESC
                  ) AS RN_PK
            FROM `{project}.insight_dev.personas_test`
            WHERE timestamp > (
                SELECT COALESCE(MAX(updated_date), TIMESTAMP('2000-01-01')) 
                FROM `the1-insight-dev.insight_dev.stg_ms_personas`
            )

        )
        WHERE RN_PK = 1
    """
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
    origin_query = f"""
        SELECT DISTINCT origin.*
        FROM `{project}.insight_dev.stg_ms_member` AS origin
        INNER JOIN `{project}.insight_dev.personas_test` AS src
        ON origin.MEMBER_NUMBER = JSON_VALUE(src.profiles, '$.memberId') 
        WHERE src.timestamp > (
            SELECT COALESCE(MAX(updated_date), TIMESTAMP('2000-01-01')) 
            FROM `the1-insight-dev.insight_dev.stg_ms_member`
        )
    """

    with beam.Pipeline(options=pipeline_options) as p:
        # mapping rows
        mapping_rows_pc = (
            p
            | "ReadMapping" >> ReadFromBigQuery(
                query=mapping_query,
                use_standard_sql=True,
                project=project,
                gcs_location=TEMP_GCS,
            )
        )
        mapping_rows_list = beam.pvalue.AsList(mapping_rows_pc)
        mapping_dict_side = (
            mapping_rows_pc
            | "CollectMappingRows" >> beam.combiners.ToList()
            | "BuildMappingDict" >> beam.Map(create_mapping_dict)
        )
        mapping_dict_side = beam.pvalue.AsSingleton(mapping_dict_side)

        # source & origin
        source_data = (
            p
            | "ReadSource" >> ReadFromBigQuery(
                query=source_query,
                use_standard_sql=True,
                project=project,
                gcs_location=TEMP_GCS,
            )
        )
        origin_data = (
            p
            | "ReadOrigin" >> ReadFromBigQuery(
                query=origin_query,
                use_standard_sql=True,
                project=project,
                gcs_location=TEMP_GCS,
            )
        )

        # map source -> target columns (new)
        mapped_new = (
            source_data
            | "MapReconcile" >> beam.Map(lambda rec, m: map_record(rec, m, "reconcile"), mapping_dict_side)
        )

        # join by PK
        joined = ({
            "new": mapped_new   | "KV_new" >> beam.Map(kv_from_new)    | "DropNoneKeyNew" >> beam.Filter(key_is_not_none),
            "old": origin_data  | "KV_old" >> beam.Map(kv_from_origin) | "DropNoneKeyOld" >> beam.Filter(key_is_not_none),
        }) | "JoinNewOld" >> beam.CoGroupByKey()

        # coalesce สองมุมมอง
        reconcile_rows = joined | "Coalesce_Reconcile" >> beam.Map(
            coalesce_by_mapping, columns=mapping_rows_list, flag_field="RECONCILE_RETRIEVED"
        )
        patch_origin_rows = joined | "Coalesce_PatchOrigin" >> beam.Map(
            coalesce_by_mapping, columns=mapping_rows_list, flag_field="RECONCILE_CONFIRMED"
        )
        reconcile_rows_casted = (
            reconcile_rows
            | 'NormalizeToFixedSchema_reconcile' >> beam.Map(normalize_row_to_schema, schema=PARQUET_SCHEMA)
        )

        member_rows_casted = (
            patch_origin_rows
            | 'NormalizeToFixedSchema_member' >> beam.Map(normalize_row_to_schema, schema=PARQUET_SCHEMA)
        )
        #  ============================== OUTPUT ==============================
        #  ============================== GCP ==============================
        _ = (reconcile_rows_casted
            | 'Write GCS Parquet' >> WriteToParquet(
                file_path_prefix=f'gs://t1-insight-data-bucket/staging/stg_ms_persosnas_test/run_dt={RUN_DT_STR}/reconcile',
                schema=PARQUET_SCHEMA,
                file_name_suffix='.snappy.parquet',
                num_shards=2
            ))
        # ===================================================================
        #  ============================== AWS ==============================
        # 4.3 เขียนไป S3 (ต้องมี S3Options/ENV AWS_* ตั้งไว้ตามที่คุยกัน)
        _ = (
            reconcile_rows_casted
            # reconcile_rows
            | 'WriteToS3_Parquet' >> WriteToParquet(
                # file_path_prefix=f's3://t1-analytics/refined/insights/ms_member_temp_dev_test/run_dt={RUN_DT_STR}/reconcile',
                file_path_prefix=f's3://t1-analytics/refined/insights/ms_personas_dev/run_dt={RUN_DT_STR}',
                # s3://t1-analytics/refined/insights/ms_personas_dev/
                schema=PARQUET_SCHEMA,
                file_name_suffix='.snappy.parquet',
                num_shards=2
            )
        )
        # _ = (
        #     member_rows_casted
        #     | 'WriteToS3_Parquet' >> WriteToParquet(
        #         file_path_prefix=f's3://t1-analytics/refined/insights/ms_member_dev/run_dt={RUN_DT_STR}',
        #         # s3://t1-analytics/refined/insights/ms_member_dev/
        #         schema=PARQUET_SCHEMA,
        #         file_name_suffix='.snappy.parquet',
        #         num_shards=2
        #     )
        # )

        # ===================================================================
        # debug counts
        _ = (source_data      | "CountSource" >> beam.combiners.Count.Globally()
                            | "LogCountSource" >> beam.Map(lambda c: LOGGER.info("Source rows = %s", c)))
        _ = (origin_data      | "CountOrigin" >> beam.combiners.Count.Globally()
                            | "LogCountOrigin" >> beam.Map(lambda c: LOGGER.info("Origin rows = %s", c)))
        _ = (reconcile_rows   | "CountRecon" >> beam.combiners.Count.Globally()
                            | "LogCountRecon" >> beam.Map(lambda c: LOGGER.info("Reconcile rows = %s", c)))
        _ = (patch_origin_rows| "CountPatch" >> beam.combiners.Count.Globally()
                            | "LogCountPatch" >> beam.Map(lambda c: LOGGER.info("Patch-origin rows = %s", c)))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
