#!/usr/bin/env python
import re
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery

logging.basicConfig(level=logging.INFO)

# ---------------------------
# Helpers: dot-path extractor (+ JSON string auto-parse)
# ---------------------------
_TOKEN_RE = re.compile(r'([^\[\]]+)|\[(\d+)\]')

def _auto_json(v):
    if isinstance(v, str):
        s = v.strip()
        if s.startswith('{') or s.startswith('['):
            try:
                return json.loads(s)
            except Exception:
                return v
    return v

def extract_by_path(record, path):
    if not path:
        return None
    cur = record
    for segment in path.split('.'):
        cur = _auto_json(cur)
        if cur is None:
            return None
        for tok in _TOKEN_RE.finditer(segment):
            key = tok.group(1)
            idx = tok.group(2)
            if key is not None:
                if isinstance(cur, dict):
                    cur = cur.get(key)
                else:
                    return None
            elif idx is not None:
                if isinstance(cur, (list, tuple)):
                    i = int(idx)
                    cur = cur[i] if -len(cur) <= i < len(cur) else None
                else:
                    return None
    return cur

# ---------------------------
# Build mapping spec from mapping table rows
# ---------------------------
def build_mapping_spec(rows):
    """
    rows: list of dict from stg_mapping_reconcile
    Returns:
      {
        'paths': {target_col: src_path},
        'retrieved_cols': [...],
        'confirmed_cols': [...]
      }
    """
    paths = {} # all columns
    retrieved, confirmed = [], [] # columns table reconcile , column table confirmed
    for r in rows:
        tgt = r.get('RECONCILE_COLUMN_NAME')
        src = r.get('PERSONAS_MAPPING_COLUMN_NAME')
        if not tgt or not src:
            continue
        paths[tgt] = src
        if r.get('RECONCILE_RETRIEVED'):
            retrieved.append(tgt)
        if r.get('RECONCILE_CONFIRMED'):
            confirmed.append(tgt)
    return {'paths': paths, 'retrieved_cols': retrieved, 'confirmed_cols': confirmed}

# ---------------------------
# Map source record -> new_message by mapping spec
# ---------------------------
def apply_mapping(record,
                  mapping_spec,
                  pk_target_field='member_number',
                  pk_source_path='profiles.memberId',
                  treat_empty_as_missing=True):
    paths = mapping_spec['paths']
    out = {}
    for tgt, src in paths.items():
        val = extract_by_path(record, src)
        if treat_empty_as_missing and isinstance(val, str) and val.strip() == '':
            val = None
        if val is not None:
            out[tgt] = val

    # Ensure PK is present (use mapped value if available, otherwise derive from raw record)
    pk_val = out.get(pk_target_field)
    if pk_val is None and pk_source_path:
        pk_val = extract_by_path(record, pk_source_path)
        if pk_val is not None:
            out[pk_target_field] = pk_val
    return out

# ---------------------------
# KV helpers (use mapped PK for 'new', origin PK for 'old')
# ---------------------------
# PK_TARGET_FIELD   = 'member_number'
# ORIGIN_PK_COLUMN  = 'member_number'

def to_kv_new(d, PK_TARGET_FIELD='member_number'):
    return (d.get(PK_TARGET_FIELD), d)

def to_kv_origin(r, ORIGIN_PK_COLUMN='member_number'):
    return (r.get(ORIGIN_PK_COLUMN), r)

# ---------------------------
# Coalesce per column list (COALESCE(new.col, old.col))
# ---------------------------
def coalesce_columns(kv, column_list, pk_field='member_number', treat_empty_as_missing=True):
    key, groups = kv
    news = groups.get('new') or []
    olds = groups.get('old') or []
    new_row = news[0] if news else {}
    old_row = olds[0] if olds else {}

    out = {pk_field: key}
    for c in column_list:
        new_val = new_row.get(c)
        if treat_empty_as_missing and isinstance(new_val, str) and new_val.strip() == '':
            new_val = None
        out[c] = new_val if new_val is not None else old_row.get(c)
    return out

# ---------------------------
# Logging helper
# ---------------------------
def log_count(prefix, count):
    logging.info(f"✅ {prefix}: {count}")

# ---------------------------
# Pipeline
# ---------------------------
def run():
    pipeline_options = PipelineOptions()
    gco = pipeline_options.view_as(GoogleCloudOptions)
    project = gco.project
    if not project:
        raise ValueError("--project ต้องถูกระบุใน PipelineOptions")

    # Queries
    source_query = f"""
        SELECT * EXCEPT(RN_PK)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER(
                     PARTITION BY JSON_VALUE(profiles, '$.memberId')
                     ORDER BY `TIMESTAMP` DESC
                   ) AS RN_PK
            FROM `{project}.insight_dev.personas_test`
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
    origin_query = f"SELECT * FROM `{project}.insight_dev.stg_ms_member`"

    with beam.Pipeline(options=pipeline_options) as p:
        # Read mapping (EXPORT)
        mapping_rows = (
            p
            | 'Read_MAPPING' >> ReadFromBigQuery(
                query=mapping_query,
                use_standard_sql=True,
                project=project,
                method=ReadFromBigQuery.Method.EXPORT,
                gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
            )
        )
        # To side inputs
        mapping_rows_list = mapping_rows | 'Map_ToList' >> beam.combiners.ToList()
        mapping_spec_pc   = mapping_rows_list | 'BuildMappingSpec' >> beam.Map(build_mapping_spec)
        mapping_spec_side = beam.pvalue.AsSingleton(mapping_spec_pc)

        retrieved_cols_pc = mapping_rows_list | 'Cols_Retrieved' >> beam.Map(
            lambda rows: [r['RECONCILE_COLUMN_NAME'] for r in rows if r.get('RECONCILE_RETRIEVED')]
        )
        confirmed_cols_pc = mapping_rows_list | 'Cols_Confirmed' >> beam.Map(
            lambda rows: [r['RECONCILE_COLUMN_NAME'] for r in rows if r.get('RECONCILE_CONFIRMED')]
        )
        retrieved_cols_side = beam.pvalue.AsSingleton(retrieved_cols_pc)
        confirmed_cols_side = beam.pvalue.AsSingleton(confirmed_cols_pc)

        # Read source (EXPORT)
        source_data = (
            p
            | 'Read_SRC' >> ReadFromBigQuery(
                query=source_query,
                use_standard_sql=True,
                project=project,
                method=ReadFromBigQuery.Method.EXPORT,
                gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
            )
        )
        _ = (source_data
             | 'Count_SRC' >> beam.combiners.Count.Globally()
             | 'Log_SRC'   >> beam.Map(lambda c: log_count('SRC rows', c)))

        # Read origin (EXPORT)
        origin_data = (
            p
            | 'Read_ORIGIN' >> ReadFromBigQuery(
                query=origin_query,
                use_standard_sql=True,
                project=project,
                method=ReadFromBigQuery.Method.EXPORT,
                gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
            )
        )
        _ = (origin_data
             | 'Count_ORIGIN' >> beam.combiners.Count.Globally()
             | 'Log_ORIGIN'   >> beam.Map(lambda c: log_count('ORIGIN rows', c)))

        # Map source -> new_message by mapping
        mapped_new = (
            source_data
            | 'ApplyMapping' >> beam.Map(
                apply_mapping,
                mapping_spec=mapping_spec_side,
                pk_target_field=PK_TARGET_FIELD,
                pk_source_path='profiles.memberId'  # ปรับให้ตรงกับจริง ถ้า path เปลี่ยน
            )
        )

        # Join by PK
        joined = ({
            'new': mapped_new | 'KV_new'    >> beam.Map(to_kv_new,PK_TARGET_FIELD=PK_TARGET_FIELD),
            'old': origin_data | 'KV_origin' >> beam.Map(to_kv_origin, ORIGIN_PK_COLUMN=ORIGIN_PK_COLUMN),
        }) | 'Join_new_old' >> beam.CoGroupByKey()

        # COALESCE ต่อคอลัมน์
        reconcile_rows = joined | 'Coalesce_RETRIEVED' >> beam.Map(
            coalesce_columns, column_list=retrieved_cols_side, pk_field=PK_TARGET_FIELD
        )
        patch_origin_rows = joined | 'Coalesce_CONFIRMED' >> beam.Map(
            coalesce_columns, column_list=confirmed_cols_side, pk_field=PK_TARGET_FIELD
        )

        # Debug counts
        _ = (reconcile_rows
             | 'Count_RECONCILE' >> beam.combiners.Count.Globally()
             | 'Log_RECONCILE'   >> beam.Map(lambda c: log_count('RECONCILE rows', c)))
        _ = (patch_origin_rows
             | 'Count_PATCH' >> beam.combiners.Count.Globally()
             | 'Log_PATCH'   >> beam.Map(lambda c: log_count('PATCH rows', c)))

if __name__ == '__main__':
    PK_TARGET_FIELD   = 'member_number'
    ORIGIN_PK_COLUMN  = 'member_number'
    logging.getLogger().setLevel(logging.INFO)
    run()
