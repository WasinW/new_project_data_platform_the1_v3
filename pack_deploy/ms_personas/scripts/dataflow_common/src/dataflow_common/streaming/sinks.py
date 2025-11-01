"""Output sinks for streaming pipelines."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import apache_beam as beam
from apache_beam.io import WriteToPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.transforms import trigger, window

LOGGER = logging.getLogger(__name__)


def default_cleanup(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in row.items() if not k.startswith("_")}


def apply_sink_config(
    pcoll: beam.PCollection,
    sink_config: Iterable[Dict[str, Any]],
    *,
    cleanup_fn=default_cleanup,
) -> Dict[str, Any]:
    """Apply a list of sink definitions to the given PCollection."""

    results: Dict[str, Any] = {}
    for idx, sink in enumerate(sink_config):
        sink_type = sink.get("type")
        label = sink.get("label", f"Sink{idx}")
        if sink_type == "bigquery":
            cleaned = pcoll | f"{label}_Cleanup" >> beam.Map(cleanup_fn)
            results[label] = cleaned | f"{label}_Write" >> WriteToBigQuery(
                table=sink["table"],
                schema=sink.get("schema", "SCHEMA_AUTODETECT"),
                create_disposition=sink.get(
                    "create_disposition", BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=sink.get(
                    "write_disposition", BigQueryDisposition.WRITE_APPEND
                ),
                method=sink.get(
                    "method", WriteToBigQuery.Method.STREAMING_INSERTS
                ),
                insert_retry_strategy=sink.get(
                    "insert_retry_strategy", "RETRY_ON_TRANSIENT_ERROR"
                ),
            )
        elif sink_type == "bigquery_cdc":
            cleaned = pcoll | f"{label}_CleanupCDC" >> beam.Map(cleanup_fn)
            result = cleaned | f"{label}_WriteCDC" >> WriteToBigQuery(
                table=sink["table"],
                schema=sink.get("schema", "SCHEMA_AUTODETECT"),
                create_disposition=sink.get(
                    "create_disposition", BigQueryDisposition.CREATE_IF_NEEDED
                ),
                write_disposition=sink.get(
                    "write_disposition", BigQueryDisposition.WRITE_APPEND
                ),
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
                use_cdc_writes=True,
                primary_key=sink.get("primary_key"),
            )
            results[label] = {
                "write_result": result,
                "failed_rows": result.failed_rows(),
                "failed_rows_with_errors": result.failed_rows_with_errors(),
            }
        elif sink_type == "pubsub":
            payload_field = sink.get("payload_field")
            if payload_field:
                branch = pcoll | f"{label}_Extract" >> beam.Map(
                    lambda row, field=payload_field: json.dumps(row.get(field, row)).encode(
                        "utf-8"
                    )
                )
            else:
                branch = pcoll | f"{label}_ToJSON" >> beam.Map(
                    lambda row: json.dumps(cleanup_fn(row)).encode("utf-8")
                )
            results[label] = branch | f"{label}_Write" >> WriteToPubSub(
                topic=sink["topic"]
            )
        elif sink_type == "s3":
            results[label] = (
                pcoll
                | f"{label}_Window" >> beam.WindowInto(
                    window.FixedWindows(sink.get("window", 3600)),
                    trigger=trigger.AfterWatermark(
                        early=trigger.AfterProcessingTime(
                            sink.get("early_firing_seconds", 60)
                        )
                    ),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING,
                )
                | f"{label}_Write" >> beam.ParDo(
                    BatchedS3ParquetWriter(
                        bucket=sink["bucket"],
                        prefix=sink["prefix"],
                        aws_access_key=sink["aws_access_key"],
                        aws_secret_key=sink["aws_secret_key"],
                        batch_size=sink.get("batch_size", 100),
                        batch_timeout=sink.get("batch_timeout", 10),
                        s3_client_factory=sink.get("s3_client_factory"),
                    )
                ).with_outputs("success", "failed")
            )
        else:
            raise ValueError(f"Unknown sink type: {sink_type}")
    return results


class BatchedS3ParquetWriter(beam.DoFn):
    """Write Parquet batches to S3 with micro-batching."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        aws_access_key: str,
        aws_secret_key: str,
        batch_size: int = 10,
        batch_timeout: int = 5,
        region_name: str = "ap-southeast-1",
        s3_client_factory=None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.region_name = region_name
        self.s3_client_factory = s3_client_factory
        self._s3_client = None
        self._buffer: list[Dict[str, Any]] = []
        self._buffer_start_time: Optional[float] = None
        self._created_partitions: set[str] = set()

    def setup(self) -> None:
        if self.s3_client_factory is not None:
            self._s3_client = self.s3_client_factory()
        else:
            import boto3

            self._s3_client = boto3.client(
                "s3",
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
            )

    def start_bundle(self) -> None:
        self._buffer = []
        self._buffer_start_time = time.time()

    def process(self, element: Dict[str, Any], window=beam.DoFn.WindowParam):  # pragma: no cover
        self._buffer.append((element, window))
        should_flush = len(self._buffer) >= self.batch_size
        if not should_flush and self._buffer_start_time is not None:
            should_flush = time.time() - self._buffer_start_time > self.batch_timeout
        if should_flush:
            yield from self._flush_buffer()

    def finish_bundle(self):  # pragma: no cover
        if self._buffer:
            yield from self._flush_buffer()

    def _flush_buffer(self):
        if not self._buffer:
            return []

        window_groups: Dict[Any, list] = defaultdict(list)
        for element, window_param in self._buffer:
            window_groups[window_param].append(element)

        outputs = []
        for window_param, elements in window_groups.items():
            window_start = window_param.start.to_utc_datetime().replace(tzinfo=timezone.utc)
            partition_path = (
                f"{self.prefix}/year={window_start.year:04d}/month={window_start.month:02d}/"
                f"day={window_start.day:02d}/hour={window_start.hour:02d}"
            )
            self._ensure_partition(partition_path)
            try:
                file_key = self._write_batch(partition_path, elements)
                outputs.append(
                    beam.pvalue.TaggedOutput(
                        "success",
                        {
                            "path": file_key,
                            "count": len(elements),
                            "written_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                )
            except Exception as exc:  # pragma: no cover - relies on boto3
                LOGGER.error("Failed to write S3 batch: %s", exc)
                outputs.append(
                    beam.pvalue.TaggedOutput(
                        "failed",
                        {
                            "error": str(exc),
                            "count": len(elements),
                        },
                    )
                )
        self._buffer = []
        self._buffer_start_time = time.time()
        return outputs

    def _write_batch(self, partition_path: str, elements: Iterable[Dict[str, Any]]) -> str:
        from io import BytesIO

        import pyarrow as pa
        import pyarrow.parquet as pq

        clean_elements = [default_cleanup(elem) for elem in elements]
        table = pa.Table.from_pylist(clean_elements)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="snappy")

        file_key = f"{partition_path}/batch_{uuid.uuid4().hex}.parquet"
        self._s3_client.put_object(
            Bucket=self.bucket,
            Key=file_key,
            Body=buffer.getvalue(),
        )
        LOGGER.info("Wrote %d records to s3://%s/%s", len(clean_elements), self.bucket, file_key)
        return file_key

    def _ensure_partition(self, partition_path: str) -> None:
        marker_key = f"{partition_path}/_SUCCESS"
        if marker_key in self._created_partitions:
            return
        body = json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "pipeline": "ms_member_streaming",
            }
        ).encode("utf-8")
        self._s3_client.put_object(Bucket=self.bucket, Key=marker_key, Body=body)
        self._created_partitions.add(marker_key)


__all__ = ["apply_sink_config", "BatchedS3ParquetWriter", "default_cleanup"]
