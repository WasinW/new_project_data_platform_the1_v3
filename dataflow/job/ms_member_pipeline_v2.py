#!/usr/bin/env python
"""
Pipeline entry point for refactored dataflow packages
Uses dataflow_builder instead of dataflow_common
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from apache_beam.options.pipeline_options import PipelineOptions

# ⚠️ เปลี่ยนจาก dataflow_common → dataflow_builder
from dataflow_builder.config import load_config
from dataflow_builder.orchestrator import Orchestrator

def read_gcs_file(path):
    """Read text file from GCS"""
    from apache_beam.io.filesystems import FileSystems
    
    try:
        with FileSystems.open(path) as f:
            content = f.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            return content.strip()
    except Exception as e:
        logging.warning(f"Failed to read cache file {path}: {e}")
        return None

def parse_args():
    """Parse arguments แยกระหว่าง custom args กับ Beam args"""
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
    parser.add_argument(
        "--cache_path",
        default="gs://t1-insight-audit-bucket/cache/ms_member_max_date.txt",
        help="Path to max_date cache file",
    )
    # Parse only known arguments, ignore Beam arguments
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def main():
    logging.basicConfig(level=logging.INFO)
    
    # Parse arguments
    args, pipeline_args = parse_args()

    # Load config from YAML
    cfg = load_config(args.config_path)
    
    # Read max_date from cache file (ถ้ามี)
    cached_max_date = read_gcs_file(args.cache_path)
    
    if cached_max_date:
        logging.info(f"Using cached max_date: {cached_max_date}")
        cfg.params.max_date = cached_max_date
    else:
        # Use default if no cache
        default_max_date = "2020-01-01 00:00:00"
        logging.info(f"No cache found, using default max_date: {default_max_date}")
        cfg.params.max_date = default_max_date
    
    # Override run_dt if supplied
    if args.run_dt:
        cfg.params.run_dt = args.run_dt
    elif not cfg.params.run_dt:
        from datetime import datetime
        now = datetime.now()
        cfg.params.run_dt = now.strftime('%Y%m%d%H')
        
        # Generate partition params
        cfg.params.run_par_month = now.strftime('%Y%m')
        cfg.params.run_par_day = now.strftime('%d')
        cfg.params.run_par_hour = now.strftime('%H')
        
    logging.info(f"Pipeline params: run_dt={cfg.params.run_dt}, max_date={cfg.params.max_date}")
    
    # Create PipelineOptions จาก pipeline_args ที่เหลือ
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Run pipeline
    orchestrator = Orchestrator(cfg)
    orchestrator.run(pipeline_options)
    
    logging.info("Pipeline submitted successfully!")

if __name__ == "__main__":
    main()