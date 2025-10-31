# dataflow_common/ms_member_streaming_pipeline.py

#!/usr/bin/env python
"""Entry point for ms_member streaming pipeline"""

import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator

def parse_args():
    """Parse arguments"""
    parser = argparse.ArgumentParser(description="Run ms_member streaming pipeline")
    parser.add_argument(
        "--config_path",
        required=True,
        help="Path to YAML configuration file"
    )
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def main():
    logging.basicConfig(level=logging.INFO)
    args, pipeline_args = parse_args()
    
    # Load config
    cfg = load_config(args.config_path)
    
    # Set streaming mode
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Run pipeline
    orchestrator = Orchestrator(cfg)
    orchestrator.run(pipeline_options)

if __name__ == "__main__":
    main()