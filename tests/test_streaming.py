import json
import unittest
from datetime import datetime
from typing import Any, Iterable

from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import Timestamp

from dataflow_common.streaming.mapping import MappingCacheLoader
from dataflow_common.streaming.message import ApplyMappingDoFn, ParsePubSubMessage, DLQ_TAG, SUCCESS_TAG
from dataflow_common.streaming.sideinputs import CachedQuerySideInput
from dataflow_common.streaming.sinks import BatchedS3ParquetWriter, apply_sink_config, default_cleanup


class FakeQueryJob:
    def __init__(self, rows: Iterable[Any]):
        self._rows = list(rows)

    def result(self):
        return self._rows


class FakeClient:
    def __init__(self, rows: Iterable[Any]):
        self.rows = list(rows)
        self.calls = 0

    def query(self, query):  # pragma: no cover - behaviour verified via side effects
        self.calls += 1
        return FakeQueryJob(self.rows)


class StreamingMappingTests(unittest.TestCase):
    def test_cached_query_side_input_caches_results(self):
        rows = [{"id": 1}, {"id": 2}]
        client = FakeClient(rows)
        loader = CachedQuerySideInput(
            query="SELECT 1",
            ttl_seconds=60,
            client_factory=lambda: client,
        )
        loader.setup()
        first = list(loader.process(None))
        second = list(loader.process(None))
        self.assertEqual(first, [rows])
        self.assertEqual(second, [rows])
        self.assertEqual(client.calls, 1, "Query should be executed only once due to caching")

    def test_mapping_cache_loader_builds_dict(self):
        rows = [
            {
                "RECONCILE_COLUMN_NAME": "member_number",
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
                "RECONCILE_RETRIEVED": True,
                "RECONCILE_CONFIRMED": False,
            }
        ]
        client = FakeClient(rows)
        loader = MappingCacheLoader(
            project_id="test",
            dataset="test",
            client_factory=lambda: client,
        )
        loader.setup()
        result = list(loader.process(None))[0]
        self.assertIn("member_number", result)
        self.assertEqual(result["member_number"]["src_path"], ["profiles", "memberId"])


class MessageProcessingTests(unittest.TestCase):
    def test_parse_pubsub_success_and_dlq(self):
        parser = ParsePubSubMessage()
        success_records = list(parser.process({"member_id": "123", "value": 1}))
        self.assertEqual(len(success_records), 1)
        self.assertEqual(success_records[0].tag, SUCCESS_TAG)
        dlq_records = list(parser.process({"value": 1}))
        self.assertEqual(dlq_records[0].tag, DLQ_TAG)

    def test_apply_mapping_with_dimension(self):
        dofn = ApplyMappingDoFn(pk_field="member_number")
        mapping = {
            "member_number": {"src_path": ["profiles", "memberId"], "reconcile": True, "original": False},
            "email": {"src_path": ["profiles", "email"], "reconcile": True, "original": True},
        }
        element = {
            "member_id": "999",
            "payload": {"profiles": {"memberId": "999", "email": "test@example.com"}},
        }
        dimension = {"999": {"status": "active"}}
        result = list(dofn.process(element, mapping, dimension_data=dimension))[0]
        self.assertEqual(result["member_number"], "999")
        self.assertEqual(result["email"], "test@example.com")
        self.assertEqual(result["status"], "active")


class S3WriterTests(unittest.TestCase):
    def test_batched_s3_writer_flushes(self):
        class FakeS3:
            def __init__(self):
                self.put_calls = []

            def put_object(self, **kwargs):
                self.put_calls.append(kwargs)

        window = IntervalWindow(Timestamp(0), Timestamp(60))
        writer = BatchedS3ParquetWriter(
            bucket="bucket",
            prefix="prefix",
            aws_access_key="key",
            aws_secret_key="secret",
            batch_size=2,
            s3_client_factory=FakeS3,
        )
        writer.setup()
        writer.start_bundle()
        outputs = list(writer.process({"foo": "bar"}, window=window))
        outputs += list(writer.process({"foo": "baz"}, window=window))
        self.assertTrue(outputs)
        success_outputs = [o for o in outputs if o.tag == "success"]
        self.assertEqual(len(success_outputs), 1)

    def test_apply_sink_config_rejects_unknown_type(self):
        with self.assertRaises(ValueError):
            apply_sink_config([], [{"type": "unknown"}])

    def test_default_cleanup_removes_internal_fields(self):
        cleaned = default_cleanup({"a": 1, "_internal": 2})
        self.assertEqual(cleaned, {"a": 1})


if __name__ == "__main__":
    unittest.main()
