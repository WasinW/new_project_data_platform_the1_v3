#!/usr/bin/env python
"""
Entry point for the ms_member_short pipeline using dataflow_common.

This script reads a YAML configuration file describing the pipeline
plan, merges any defaults and overrides, and uses the
dataflow_common orchestrator to construct and run the Beam
pipeline.  It is suitable for execution both locally and on
DataflowRunner when passed appropriate pipeline options via the
command line.
"""

from __future__ import annotations

import argparse
import logging

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ms_member_short pipeline")
    parser.add_argument(
        "--config_path",
        required=True,
        help="Path to the YAML configuration file for this pipeline",
    )
    parser.add_argument(
        "--run_dt",
        default=None,
        help="Run date (YYYY-MM-DD) to embed in output paths and params",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    # Load config from YAML
    cfg = load_config(args.config_path)
    # Override run_dt if supplied
    if args.run_dt:
        cfg.params.run_dt = args.run_dt
    # Save main session so that Beam can serialize global context on Dataflow
    pipeline_options = PipelineOptions()
    setup_opts = pipeline_options.view_as(SetupOptions)
    setup_opts.save_main_session = True
    # Run pipeline
    orchestrator = Orchestrator(cfg)
    orchestrator.run(pipeline_options)


if __name__ == "__main__":
    main()