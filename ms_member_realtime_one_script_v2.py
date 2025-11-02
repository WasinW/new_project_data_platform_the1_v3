#!/usr/bin/env python
"""
Real-time streaming pipeline for MS Member data — Revised from your original
Adds:
  - Step 5: Demand-driven BigQuery dimension side input (windowed cache, only keys needed)
  - Step 6: Enrich mapped records using the dimension side input (post-mapping)
Keeps:
  - Step 1–4 flow identical (Mapping side input → Pub/Sub w/ DLQ → Bigtable Enrichment → Mapping)
  - Detailed step comments and DLQ handling
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window, trigger
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.periodicsequence import PeriodicSequence
from apache_beam.transforms.util import AsDict, BatchElements
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================
# CONFIGURATION (kept from your original, with a few additions)
# =============================================================
PROJECT_ID = "the1-insight-dev"
DATASET = "insight_dev"
REGION = "asia-southeast1"

# Pub/Sub settings
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/personas-updates"
DLQ_TOPIC = f"projects/{PROJECT_ID}/topics/personas-dlq"

# BigTable settings
BT_INSTANCE = "personas-instance"
BT_TABLE = "personas-data"

# Output settings
BQ_OUTPUT_TABLE = f"{PROJECT_ID}.{DATASET}.ms_personas_streaming"
S3_BUCKET = "t1-analytics"
S3_PREFIX = "refined/insights/ms_personas_streaming"

# AWS credentials (should use Secret Manager in production)
AWS_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY"
AWS_SECRET_KEY = "YOUR_AWS_SECRET_KEY"

# --- NEW: Dimension join controls (post-mapping) ---
DIM_JOIN_ENABLED = True
# BigQuery dimension table to join after mapping
DIM_TABLE = f"{PROJECT_ID}.{DATASET}.dim_member_status"  # <== change to your table
# Join key present in *mapped_records* (after mapping)
DIM_KEY_FIELD = "member_number"  # <== key to match mapped_records
# Fields to fetch from DIM_TABLE; keep first one as the key too if you want
DIM_FIELDS = ["member_number", "status_code", "vip_flag", "updated_at"]
# Refresh & window alignment for the side input cache
DIM_REFRESH_MINUTES = 15
# Merge policy: "record_wins" or "dim_wins"
DIM_MERGE_POLICY = "record_wins"

# Optional BigQuery schema placeholder for certain sinks
BQ_SCHEMA_AUTODETECT = "SCHEMA_AUTODETECT"

# ============================================
# PART 1: Mapping Side Input (unchanged in spirit)
# ============================================

def create_mapping_query() -> str:
    return f"""
    SELECT RECONCILE_COLUMN_NAME,
           PERSONAS_MAPPING_COLUMN_NAME,
           RECONCILE_RETRIEVED,
           RECONCILE_CONFIRMED
    FROM `{PROJECT_ID}.{DATASET}.stg_mapping_reconcile`
    WHERE COALESCE(UPDATED_DATE, '1999-12-31') = (
        SELECT COALESCE(MAX(UPDATED_DATE), '1999-12-31')
        FROM `{PROJECT_ID}.{DATASET}.stg_mapping_reconcile`
    )
    """

class BuildMappingDict(beam.CombineFn):
    def create_accumulator(self):
        return {}
    def add_input(self, acc, row):
        dest_col = row.get('RECONCILE_COLUMN_NAME')
        src_col = row.get('PERSONAS_MAPPING_COLUMN_NAME')
        if dest_col and src_col:
            acc[dest_col] = {
                'src_path': src_col.split('.'),
                'reconcile': bool(row.get('RECONCILE_RETRIEVED')),
                'confirmed': bool(row.get('RECONCILE_CONFIRMED'))
            }
        return acc
    def merge_accumulators(self, accs):
        out = {}
        for a in accs:
            out.update(a)
        return out
    def extract_output(self, acc):
        return acc

class QueryMappingWithCache(beam.DoFn):
    """Simplified mapping loader with TTL cache (as in your original)."""
    _cache = None
    _cache_time = 0
    CACHE_TTL = 3600
    def __init__(self):
        from google.cloud import bigquery
        self._bq_client = None
    def setup(self):
        from google.cloud import bigquery
        self._bq_client = bigquery.Client(project=PROJECT_ID)
    def process(self, _):
        now = time.time()
        if QueryMappingWithCache._cache and (now - QueryMappingWithCache._cache_time < self.CACHE_TTL):
            yield QueryMappingWithCache._cache
            return
        rows = list(self._bq_client.query(create_mapping_query()).result())
        md = {}
        for r in rows:
            dest_col = r.RECONCILE_COLUMN_NAME
            src_col = r.PERSONAS_MAPPING_COLUMN_NAME
            if dest_col and src_col:
                md[dest_col] = {
                    'src_path': src_col.split('.'),
                    'reconcile': bool(r.RECONCILE_RETRIEVED),
                    'confirmed': bool(r.RECONCILE_CONFIRMED)
                }
        QueryMappingWithCache._cache = md
        QueryMappingWithCache._cache_time = now
        logger.info(f"Updated mapping cache with {len(md)} entries")
        yield md

# ============================================
# PART 2: BigTable Enrichment (kept, with fallback)
# ============================================
try:
    from apache_beam.transforms.enrichment import Enrichment
    from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
    def create_bigtable_enrichment():
        return BigTableEnrichmentHandler(
            project_id=PROJECT_ID,
            instance_id=BT_INSTANCE,
            table_id=BT_TABLE,
            row_key_fn=lambda e: f"member#{e['member_id']}".encode(),
            column_families=['profiles', 'attributes']
        )
    USE_ENRICHMENT_API = True
except Exception:
    USE_ENRICHMENT_API = False
    logger.warning("Enrichment API not available, using fallback BigTable lookup")

class OptimizedBigTableLookup(beam.DoFn):
    def __init__(self, project_id, instance_id, table_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self._table = None
    def setup(self):
        from google.cloud import bigtable
        client = bigtable.Client(project=self.project_id)
        instance = client.instance(self.instance_id)
        self._table = instance.table(self.table_id)
    def process(self, batch_elements):
        if not batch_elements:
            return
        row_keys, key_to_elem = [], {}
        for elem in batch_elements:
            mid = elem.get('member_id')
            if mid:
                rk = f"member#{mid}".encode()
                row_keys.append(rk)
                key_to_elem[rk.decode()] = elem
        if not row_keys:
            return
        from google.cloud.bigtable.row_set import RowSet
        rs = RowSet()
        for k in row_keys:
            rs.add_row_key(k)
        rows = self._table.read_rows(row_set=rs)
        found = set()
        for row in rows:
            rk = row.row_key.decode('utf-8')
            found.add(rk)
            mid = rk.replace('member#', '')
            enriched = {'member_number': mid}
            for fam, cols in row.cells.items():
                for col, cells in cols.items():
                    name = col.decode('utf-8')
                    val = cells[0].value
                    try:
                        sval = val.decode('utf-8')
                        try:
                            val = json.loads(sval)
                        except Exception:
                            val = sval
                    except Exception:
                        pass
                    enriched[f"{fam}:{name}"] = val
            yield enriched
        for rk, elem in key_to_elem.items():
            if rk not in found:
                yield {'member_number': elem.get('member_id'), 'found_in_bigtable': False}

# ============================================
# PART 3: Message Processing & DLQ (kept)
# ============================================
class ProcessPubSubMessage(beam.DoFn):
    def process(self, element):
        try:
            if hasattr(element, 'data'):
                data = element.data
                data = data.decode('utf-8') if isinstance(data, (bytes, bytearray)) else data
            else:
                data = element if isinstance(element, str) else json.dumps(element)
            msg = json.loads(data) if isinstance(data, str) else data
            member_id = (
                msg.get('member_id') or msg.get('memberId') or
                (json.loads(msg['profiles']).get('memberId') if isinstance(msg.get('profiles'), str) else (msg.get('profiles') or {}).get('memberId')) or
                (json.loads(msg['payload']).get('member_id') if isinstance(msg.get('payload'), str) else (msg.get('payload') or {}).get('member_id'))
            )
            if not member_id:
                raise ValueError('No member_id found in message')
            yield beam.pvalue.TaggedOutput('success', {
                'member_id': member_id,
                'message': msg,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            yield beam.pvalue.TaggedOutput('dlq', {
                'data': str(element),
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })

class ApplyMappingOptimized(beam.DoFn):
    def process(self, element, mapping_dict):
        if not mapping_dict:
            yield element
            return
        mapped = {}
        for dest_col, cfg in mapping_dict.items():
            path = cfg.get('src_path', [])
            val = element
            for k in path:
                if isinstance(val, dict):
                    val = val.get(k)
                else:
                    val = None
                    break
            mapped[dest_col] = val
        if 'member_number' not in mapped:
            mapped['member_number'] = element.get('member_number') or element.get('member_id')
        mapped['_processed_at'] = datetime.now().isoformat()
        yield mapped

# ============================================
# PART 4: Optimized S3 Writer with Micro‑batching (kept)
# ============================================
class BatchedS3ParquetWriter(beam.DoFn):
    def __init__(self, bucket, prefix, aws_access_key, aws_secret_key, batch_size=100, batch_timeout=10):
        self.bucket = bucket
        self.prefix = prefix
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self._s3 = None
        self._buf = []
        self._t0 = None
        self._done_parts = set()
    def setup(self):
        self._s3 = boto3.client('s3', region_name='ap-southeast-1',
                                aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
    def start_bundle(self):
        self._buf = []
        self._t0 = time.time()
    def process(self, element, window=beam.DoFn.WindowParam):
        self._buf.append((element, window))
        if len(self._buf) >= self.batch_size or (self._t0 and time.time() - self._t0 > self.batch_timeout):
            yield from self._flush()
    def finish_bundle(self):
        if self._buf:
            yield from self._flush()
    def _flush(self):
        if not self._buf:
            return
        from collections import defaultdict
        groups = defaultdict(list)
        for e, w in self._buf:
            groups[w].append(e)
        for w, elems in groups.items():
            ws = w.start.to_utc_datetime()
            path = f"{self.prefix}/year={ws.year:04d}/month={ws.month:02d}/day={ws.day:02d}/hour={ws.hour:02d}"
            self._ensure_partition(path)
            try:
                key = f"{path}/batch_{uuid.uuid4().hex}_{int(time.time())}.parquet"
                clean = [{k: v for k, v in e.items() if not k.startswith('_')} for e in elems]
                table = pa.Table.from_pylist(clean)
                import io
                buf = io.BytesIO()
                pq.write_table(table, buf, compression='snappy')
                self._s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
                yield beam.pvalue.TaggedOutput('success', {'path': key, 'count': len(elems), 'timestamp': datetime.now().isoformat()})
            except Exception as ex:
                yield beam.pvalue.TaggedOutput('failed', {'error': str(ex), 'count': len(elems)})
        self._buf = []
        self._t0 = time.time()
    def _ensure_partition(self, path):
        mk = f"{path}/_SUCCESS"
        if mk in self._done_parts:
            return
        try:
            self._s3.put_object(Bucket=self.bucket, Key=mk, Body=json.dumps({'created_at': datetime.now().isoformat()}).encode('utf-8'))
            self._done_parts.add(mk)
        except Exception as ex:
            logger.error(f"Partition marker failed: {ex}")

# ============================================
# PART 5: NEW — Demand‑driven Dimension Side Input (post‑mapping)
# ============================================

def _build_dim_query(dim_table: str, key_field: str, fields: List[str]) -> str:
    cols = ", ".join([f"`{c}`" for c in fields])
    return f"SELECT {cols} FROM `{dim_table}` WHERE `{key_field}` IN UNNEST(@keys)"

class FetchDimensionBatch(beam.DoFn):
    """Query BigQuery for a batch of keys and yield (key, row_dict)."""
    def __init__(self, project_id: str, dim_table: str, key_field: str, fields: List[str]):
        self.project_id = project_id
        self.dim_table = dim_table
        self.key_field = key_field
        self.fields = fields
        self._client = None
        self._sql = _build_dim_query(dim_table, key_field, fields)
    def setup(self):
        from google.cloud import bigquery
        self._client = bigquery.Client(project=self.project_id)
    def process(self, keys: List[str]):
        try:
            if not keys:
                return
            # de‑dup and avoid overly large arrays
            uniq = list(dict.fromkeys([k for k in keys if k]))
            if not uniq:
                return
            job = self._client.query(self._sql, job_config=self._make_job_config(uniq))
            for row in job.result():
                row_dict = {f: row[f] for f in self.fields if f in row}
                k = str(row[self.key_field])
                yield (k, row_dict)
        except Exception as ex:
            # side output 'dim_dlq'
            yield beam.pvalue.TaggedOutput('dim_dlq', {'error': str(ex), 'keys': keys[:50], 'ts': datetime.now().isoformat()})
    @staticmethod
    def _make_job_config(keys: List[str]):
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter, ArrayQueryParameter
        return QueryJobConfig(
            query_parameters=[ArrayQueryParameter('keys', 'STRING', keys)]
        )


def merge_dim(record: Dict[str, Any], dim_map: Dict[str, Dict[str, Any]], key_field: str, policy: str = 'record_wins') -> Dict[str, Any]:
    k = record.get(key_field)
    dim = dim_map.get(k) if k is not None else None
    if not dim:
        return record
    if policy == 'dim_wins':
        merged = {**record, **dim}
    else:  # record_wins
        merged = {**dim, **record}
    return merged

# ============================================
# PART 6: Sinks (router similar to your helper, kept concise)
# ============================================

def write_sink(pcoll, mode, opts):
    if mode == "native_cdc":
        return pcoll | "BQ_CDC" >> WriteToBigQuery(
            table=opts["bq_table_ms_personas"],
            schema=opts.get("schema", BQ_SCHEMA_AUTODETECT),
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            use_cdc_writes=True,
            primary_key=opts["primary_key"],
        )
    elif mode == "biglake_append":
        return pcoll | "BQ_BigLake_Append" >> WriteToBigQuery(
            table=opts["bq_table_ms_personas_ext_apd"],
            schema=opts.get("schema", BQ_SCHEMA_AUTODETECT),
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )
    elif mode == "parquet_s3":
        return (pcoll
                | 'WinForS3' >> beam.WindowInto(
                    window.FixedWindows(3600),
                    trigger=trigger.AfterWatermark(early_firings=trigger.AfterProcessingTime(60)),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING)
                | 'WriteToS3Batched' >> beam.ParDo(
                    BatchedS3ParquetWriter(S3_BUCKET, S3_PREFIX, AWS_ACCESS_KEY, AWS_SECRET_KEY)
                ).with_outputs('success', 'failed'))
    else:
        raise ValueError(f"Unknown sink mode: {mode}")

# ============================================
# MAIN PIPELINE — preserves your Step 1–4, inserts Step 5–6, then sinks
# ============================================

def run_streaming_pipeline():
    options = PipelineOptions([
        f'--project={PROJECT_ID}',
        f'--region={REGION}',
        '--runner=DataflowRunner',
        '--streaming',
        '--enable_streaming_engine',
        '--worker_machine_type=n1-standard-2',
        '--max_num_workers=10',
        '--autoscaling_algorithm=THROUGHPUT_BASED',
        '--use_public_ips=false',
        '--enable_hot_key_logging',
        f'--temp_location=gs://{PROJECT_ID}-dataflow-temp/temp',
        f'--staging_location=gs://{PROJECT_ID}-dataflow-temp/staging',
    ])

    with beam.Pipeline(options=options) as p:
        # -------------------------------------------------------------
        # Step 1: Create mapping side input (updates every hour)
        # -------------------------------------------------------------
        mapping_side_input = (
            p
            | 'TriggerMappingUpdate' >> beam.transforms.PeriodicImpulse(fire_interval=3600)
            | 'QueryMappingWithCache' >> beam.ParDo(QueryMappingWithCache())
            | 'MapWinGlobal' >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING)
        )

        # -------------------------------------------------------------
        # Step 2: Consume from Pub/Sub (+ DLQ handling)
        # -------------------------------------------------------------
        messages = (
            p
            | 'ReadFromPubSub' >> ReadFromPubSub(
                subscription=SUBSCRIPTION,
                with_attributes=True,
                id_label='message_id',
                timestamp_attribute='publish_time'
            )
            | 'ProcessMessages' >> beam.ParDo(ProcessPubSubMessage()).with_outputs('success', 'dlq')
        )
        _ = (messages.dlq | 'SerializeDLQ' >> beam.Map(json.dumps) | 'WriteToDLQ' >> WriteToPubSub(topic=DLQ_TOPIC))

        # -------------------------------------------------------------
        # Step 3: Enrich from BigTable (Enrichment API or fallback)
        # -------------------------------------------------------------
        if USE_ENRICHMENT_API:
            enriched = (messages.success | 'EnrichWithAPI' >> Enrichment(create_bigtable_enrichment()))
        else:
            enriched = (
                messages.success
                | 'BatchForBigTable' >> beam.BatchElements(min_batch_size=10, max_batch_size=100, max_latency_secs=1)
                | 'LookupBigTable' >> beam.ParDo(OptimizedBigTableLookup(PROJECT_ID, BT_INSTANCE, BT_TABLE))
            )

        # -------------------------------------------------------------
        # Step 4: Apply mapping with side input (rename to target schema)
        # -------------------------------------------------------------
        mapped_records = (
            enriched
            | 'ApplyMapping' >> beam.ParDo(ApplyMappingOptimized(), mapping_dict=beam.pvalue.AsSingleton(mapping_side_input))
        )

        # -------------------------------------------------------------
        # Step 5 (NEW): Build Dimension Side Input AFTER mapping
        #   - Demand‑driven: only fetch keys that appear in mapped_records
        #   - Windowed cache: refresh every DIM_REFRESH_MINUTES
        # -------------------------------------------------------------
        if DIM_JOIN_ENABLED:
            refresh_secs = int(DIM_REFRESH_MINUTES * 60)

            # 5.1 Extract join keys and group/deduplicate per window
            dim_keys = (
                mapped_records
                | 'WinForDimKeys' >> beam.WindowInto(window.FixedWindows(refresh_secs))
                | 'ExtractDimKey' >> beam.Map(lambda r: r.get(DIM_KEY_FIELD))
                | 'FilterNullDimKey' >> beam.Filter(lambda k: k is not None)
                | 'DedupDimKey' >> beam.Distinct()
                | 'BatchDimKeys' >> BatchElements(min_batch_size=100, max_batch_size=1000, max_latency_secs=30)
            )

            # 5.2 Query BQ for batched keys → (key, dim_row)
            dim_pairs = (
                dim_keys
                | 'FetchDimBatch' >> beam.ParDo(FetchDimensionBatch(PROJECT_ID, DIM_TABLE, DIM_KEY_FIELD, DIM_FIELDS)).with_outputs('dim_dlq', main='pairs')
            )
            dim_pairs_main = dim_pairs.pairs
            dim_pairs_dlq = dim_pairs.dim_dlq
            _ = (dim_pairs_dlq | 'LogDimDLQ' >> beam.Map(lambda x: logger.error(f"DIM DLQ: {x}")))

            # 5.3 Materialize as a side input dict per window
            dim_side = dim_pairs_main | 'DimAsDict' >> AsDict()

            # ---------------------------------------------------------
            # Step 6 (NEW): Join with Dimension Side Input (post-mapping)
            # ---------------------------------------------------------
            joined_records = (
                mapped_records
                | 'WinAlignMappedForDim' >> beam.WindowInto(window.FixedWindows(refresh_secs))
                | 'JoinDimSideInput' >> beam.Map(merge_dim, dim_map=dim_side, key_field=DIM_KEY_FIELD, policy=DIM_MERGE_POLICY)
            )
        else:
            joined_records = mapped_records

        # -------------------------------------------------------------
        # Step 7: Sinks — keep your modes (CDC, BigLake append, S3 parquet)
        #   NOTE: Toggle which sinks you want to activate
        # -------------------------------------------------------------
        # 7.1 Example: Streaming inserts (simple append)
        _ = (
            joined_records
            | 'PrepareForBQ' >> beam.Map(lambda x: {k: v for k, v in x.items() if not k.startswith('_')})
            | 'WriteToBigQuerySimple' >> WriteToBigQuery(
                table=BQ_OUTPUT_TABLE,
                schema=BQ_SCHEMA_AUTODETECT,
                write_disposition='WRITE_APPEND',
                create_disposition='CREATE_IF_NEEDED',
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
                insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR')
        )

        # 7.2 Example: CDC → Native Table (Storage Write API)
        # _ = write_sink(joined_records, 'native_cdc', {
        #     'bq_table_ms_personas': f"{PROJECT_ID}.{DATASET}.ms_personas_ntv_cdc",
        #     'primary_key': [DIM_KEY_FIELD],
        #     'schema': BQ_SCHEMA_AUTODETECT,
        # })

        # 7.3 Example: Append → BigLake Iceberg (Storage Write API)
        # _ = write_sink(joined_records, 'biglake_append', {
        #     'bq_table_ms_personas_ext_apd': f"{PROJECT_ID}.{DATASET}.ms_personas_ext_apd",
        #     'schema': BQ_SCHEMA_AUTODETECT,
        # })

        # 7.4 Example: Parquet on S3 (windowed micro-batch)
        # s3_result = write_sink(joined_records, 'parquet_s3', {})
        # _ = (s3_result.success | 'LogS3Success' >> beam.Map(lambda x: logger.info(f"S3 OK: {x['count']}")))
        # _ = (s3_result.failed  | 'LogS3Failed'  >> beam.Map(lambda x: logger.error(f"S3 ERR: {x['error']}")))

    logger.info("Streaming pipeline started with post-mapping dimension join")

if __name__ == '__main__':
    run_streaming_pipeline()
