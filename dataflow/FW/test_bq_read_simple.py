#!/usr/bin/env python
"""
Simple BigQuery Read Test - ทดสอบอ่านข้อมูลจาก BigQuery
รองรับทั้ง DirectRunner และ DataflowRunner
"""

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions, SetupOptions
# from apache_beam.io import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery

# Setup logging
logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


def log_count(count):
    """Helper function to log count results"""
    logging.info(f"✅ SUCCESS: Got {count} records from BigQuery")
    return count

def run():
    """Run the pipeline with proper project handling"""
    
    # Parse all arguments
    pipeline_options = PipelineOptions()
    
    # Get the Google Cloud options view
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    
    # The project should already be set via --project parameter from DAG
    project = google_cloud_options.project
    
    if not project:
        raise ValueError("Project must be specified via --project parameter")
    
    logging.info(f"Using project: {project}")
    logging.info(f"Pipeline options: {pipeline_options.get_all_options()}")
    
    # Build the query
    query = f"""
    SELECT *
    FROM `{project}.insight_dev.personas_test`
    LIMIT 100
    """
    
    logging.info(f"Query: {query}")
    
    # Run the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        result = (
            p 
            | 'ReadFromBigQuery' >> ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                project=project,
                gcs_location='gs://t1-insight-audit-bucket/audit_log/dataflow/temp'
            )
            | 'Count' >> beam.combiners.Count.Globally()
            | 'LogResults' >> beam.Map(log_count)  # Use function instead of lambda
        )
    
    logging.info("Pipeline completed successfully")

if __name__ == '__main__':
    # run_simple_test()
    logging.getLogger().setLevel(logging.INFO)
    run()
