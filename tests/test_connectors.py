"""
Test cases for connectors
"""
import unittest
from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from dataflow_common.connectors import (
    BigQueryConnector,
    ParquetConnector,
    GCSFilesStorage
)
from dataflow_common.config import PipelineConfig

class TestConnectorsModule(unittest.TestCase):
    """Test connector classes"""

    class _PassThrough(beam.PTransform):
        def expand(self, pcoll):
            return pcoll

    def setUp(self):
        """Set up test config"""
        self.config_dict = {
            "pipeline": {
                "name": "test_connectors",
                "mode": "batch",
                "term": "short"
            },
            "io": {
                "bq": {
                    "project": "test-project",
                    "dataset": "test_dataset",
                    "temp_gcs": "gs://temp-bucket/temp"
                },
                "s3": {
                    "refined_prefix": "s3://bucket/data",
                    "num_shards": 2
                }
            }
        }
        
        self.config = PipelineConfig.from_dict(self.config_dict)
    
    @patch('dataflow_common.connectors.ReadFromBigQuery')
    def test_bigquery_read(self, mock_read_bq):
        """Test BigQuery connector read"""
        print("\n🔬 Test: BigQuery read")
        
        with TestPipeline() as pipeline:
            query = "SELECT * FROM table"
            
            # Mock return value
            mock_read_bq.return_value = self._PassThrough()
            
            result = BigQueryConnector.read_query(
                pipeline, query, self.config, "TestRead"
            )
            
            # Verify call
            mock_read_bq.assert_called_once_with(
                query=query,
                use_standard_sql=True,
                project="test-project",
                gcs_location="gs://temp-bucket/temp"
            )
            
            print(f"   ✅ BigQuery read configured correctly")
    
    @patch('dataflow_common.connectors.WriteToBigQuery')
    def test_bigquery_write(self, mock_write_bq):
        """Test BigQuery connector write"""
        print("\n🔬 Test: BigQuery write")
        
        with TestPipeline() as p:
            data = p | beam.Create([{"id": 1}, {"id": 2}])

            mock_write_bq.return_value = self._PassThrough()

            BigQueryConnector.write(
                data,
                "output_table",
                self.config,
                method="STREAMING_INSERTS"
            )
            
            self.assertTrue(mock_write_bq.called)
            print(f"   ✅ BigQuery write configured")
    
    @patch('dataflow_common.connectors.WriteToParquet')
    @patch('dataflow_common.transforms.schema.load_schema_from_spec')
    def test_parquet_write(self, mock_load_schema, mock_write_parquet):
        """Test Parquet connector write"""
        print("\n🔬 Test: Parquet write")
        
        # Mock schema
        import pyarrow as pa
        mock_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        mock_load_schema.return_value = mock_schema
        
        with TestPipeline() as p:
            data = p | beam.Create([{"id": 1, "name": "test"}])

            mock_write_parquet.return_value = self._PassThrough()

            ParquetConnector.write(
                data,
                "gs://bucket/output",
                self.config,
                "TestWrite"
            )
            
            mock_write_parquet.assert_called_once()
            print(f"   ✅ Parquet write configured with schema")
    
    @patch('apache_beam.io.WriteToText')
    def test_gcs_write_text(self, mock_write_text):
        """Test GCS text file write"""
        print("\n🔬 Test: GCS text write")
        
        with TestPipeline() as p:
            data = p | beam.Create(["line1", "line2"])

            mock_write_text.return_value = self._PassThrough()

            GCSFilesStorage.write_text(
                data,
                "gs://bucket/output.txt",
                "TestWriteText"
            )
            
            self.assertTrue(mock_write_text.called)
            print(f"   ✅ GCS text write configured")
    
    # แก้ patch path จาก 'beam' เป็น 'apache_beam'
    @patch('apache_beam.io.ReadFromText')
    def test_gcs_read_text(self, mock_read_text):
        """Test GCS text file read"""
        print("\n🔬 Test: GCS text read")
        
        with TestPipeline() as pipeline:
            mock_read_text.return_value = beam.Create(["line1", "line2"])
            
            result = GCSFilesStorage.read_text(
                pipeline,
                "gs://bucket/input.txt",
                "TestReadText"
            )
            
            self.assertTrue(mock_read_text.called)
            print(f"   ✅ GCS text read configured")

if __name__ == "__main__":
    unittest.main()