# test_pipeline_local.py
import logging
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)

# Load config
cfg = load_config("configs/ms_member_short.yaml")

# Set test values
cfg.params.run_dt = "2025-01-07"

# Use DirectRunner for local testing
options = PipelineOptions([
    '--runner=DirectRunner',
    '--project=the1-insight-dev',
    '--temp_location=gs://t1-insight-audit-bucket/audit_log/dataflow/temp',
])

# Run locally
orchestrator = Orchestrator(cfg)
orchestrator.run(options)