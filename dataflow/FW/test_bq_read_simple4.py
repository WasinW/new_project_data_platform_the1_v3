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
from datetime import datetime, timezone

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
