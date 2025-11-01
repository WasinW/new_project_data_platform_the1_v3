#!/usr/bin/env python
"""Standalone real-time pipeline for MS member personas.

This script keeps the convenience of a single entry point for
local testing while delegating the heavy lifting to reusable
modules in :mod:`dataflow_common.streaming`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

from dataflow_common.streaming import (
    ApplyMappingDoFn,
    MappingCacheLoader,
    ParsePubSubMessage,
    apply_bigtable_enrichment,
    apply_sink_config,
    cached_side_input,
    mapping_side_input,
)
from dataflow_common.streaming.message import DLQ_TAG, SUCCESS_TAG
from dataflow_common.streaming.sideinputs import CachedQuerySideInput

LOGGER = logging.getLogger(__name__)


def _default_env(name: str, fallback: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, fallback)


def parse_args(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(description="MS Member realtime pipeline")
    parser.add_argument("--project_id", default="the1-insight-dev")
    parser.add_argument("--dataset", default="insight_dev")
    parser.add_argument("--region", default="asia-southeast1")
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--dlq_topic", required=True)
    parser.add_argument("--bq_table", required=True, help="Streaming target table")
    parser.add_argument("--cdc_table", help="CDC BigQuery table")
    parser.add_argument("--cdc_primary_key", default="member_number")
    parser.add_argument("--biglake_table", help="BigLake append table")
    parser.add_argument("--bigtable_instance")
    parser.add_argument("--bigtable_table")
    parser.add_argument("--mapping_cache_ttl", type=int, default=3600)
    parser.add_argument("--mapping_table", default="stg_mapping_reconcile")
    parser.add_argument("--id_fields", default="member_id,memberId")
    parser.add_argument("--mapping_mode", default="reconcile")
    parser.add_argument("--pk_field", default="member_number")
    parser.add_argument("--dimension_query")
    parser.add_argument("--dimension_key", default="member_number")
    parser.add_argument("--dimension_cache_ttl", type=int, default=900)
    parser.add_argument("--s3_bucket")
    parser.add_argument("--s3_prefix")
    parser.add_argument("--aws_access_key", default=_default_env("AWS_ACCESS_KEY_ID"))
    parser.add_argument("--aws_secret_key", default=_default_env("AWS_SECRET_ACCESS_KEY"))
    parser.add_argument("--s3_batch_size", type=int, default=100)
    parser.add_argument("--s3_batch_timeout", type=int, default=10)
    parser.add_argument("--log_level", default="INFO")

    known_args, pipeline_args = parser.parse_known_args(argv)
    return known_args, pipeline_args


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def build_dimension_side_input(p: beam.Pipeline, args):
    if not args.dimension_query:
        return None

    def _to_lookup(rows):
        lookup = {}
        for row in rows:
            key = str(row[args.dimension_key])
            lookup[key] = dict(row)
        return lookup

    loader = CachedQuerySideInput(
        query=args.dimension_query,
        ttl_seconds=args.dimension_cache_ttl,
        project_id=args.project_id,
        transform_rows=_to_lookup,
    )
    return cached_side_input(p, loader, seed_label="DimensionSeed", fetch_label="DimensionFetch")


def build_sink_config(args) -> List[Dict[str, Any]]:
    sinks: List[Dict[str, Any]] = [
        {
            "type": "bigquery",
            "label": "StreamingBQ",
            "table": args.bq_table,
        }
    ]

    if args.cdc_table:
        sinks.append(
            {
                "type": "bigquery_cdc",
                "label": "CDC",
                "table": args.cdc_table,
                "primary_key": [args.cdc_primary_key],
            }
        )

    if args.biglake_table:
        sinks.append(
            {
                "type": "bigquery",
                "label": "BigLake",
                "table": args.biglake_table,
                "method": WriteToBigQuery.Method.STORAGE_WRITE_API,
            }
        )

    if args.s3_bucket and args.aws_access_key and args.aws_secret_key:
        sinks.append(
            {
                "type": "s3",
                "label": "S3",
                "bucket": args.s3_bucket,
                "prefix": args.s3_prefix or "refined/ms_member_streaming",
                "aws_access_key": args.aws_access_key,
                "aws_secret_key": args.aws_secret_key,
                "batch_size": args.s3_batch_size,
                "batch_timeout": args.s3_batch_timeout,
            }
        )

    return sinks


def run(argv: Optional[List[str]] = None) -> None:
    args, pipeline_args = parse_args(argv)
    setup_logging(args.log_level)
    LOGGER.info("Starting realtime pipeline with args: %s", args)

    pipeline_options = PipelineOptions(pipeline_args)
    std_opts = pipeline_options.view_as(StandardOptions)
    std_opts.streaming = True
    setup_opts = pipeline_options.view_as(SetupOptions)
    setup_opts.save_main_session = True

    mapping_loader = MappingCacheLoader(
        project_id=args.project_id,
        dataset=args.dataset,
        table=args.mapping_table,
        ttl_seconds=args.mapping_cache_ttl,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        mapping_side = mapping_side_input(p, mapping_loader)
        dimension_side = build_dimension_side_input(p, args)

        raw_messages = p | "ReadPubSub" >> ReadFromPubSub(
            subscription=args.subscription,
            with_attributes=True,
        )

        parsed = raw_messages | "ParseMessages" >> beam.ParDo(
            ParsePubSubMessage(id_fields=[f.strip() for f in args.id_fields.split(",") if f.strip()])
        ).with_outputs(SUCCESS_TAG, DLQ_TAG)

        if args.dlq_topic:
            _ = (
                parsed[DLQ_TAG]
                | "DLQToJSON" >> beam.Map(json.dumps)
                | "WriteDLQ" >> WriteToPubSub(topic=args.dlq_topic)
            )

        enriched = parsed[SUCCESS_TAG]
        if args.bigtable_instance and args.bigtable_table:
            enriched = apply_bigtable_enrichment(
                enriched,
                project_id=args.project_id,
                instance_id=args.bigtable_instance,
                table_id=args.bigtable_table,
                row_key_fn=lambda elem: f"member#{elem['member_id']}".encode("utf-8"),
                column_families=None,
            )

        if dimension_side is not None:
            mapped = enriched | "ApplyMapping" >> beam.ParDo(
                ApplyMappingDoFn(mode=args.mapping_mode, pk_field=args.pk_field),
                mapping_dict=mapping_side,
                dimension_data=dimension_side,
            )
        else:
            mapped = enriched | "ApplyMapping" >> beam.ParDo(
                ApplyMappingDoFn(mode=args.mapping_mode, pk_field=args.pk_field),
                mapping_dict=mapping_side,
            )

        sink_config = build_sink_config(args)
        _ = apply_sink_config(mapped, sink_config)

    LOGGER.info("Pipeline finished initialisation")


if __name__ == "__main__":
    run()
