# steps/streaming.py
import json
import logging
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms import window
from ..core import BaseStep
from ..connectors.pubsub import PubSubConnector
from typing import Dict, Any, Optional
from ..connectors.bigtable import BigTableConnector
import json
import logging

LOGGER = logging.getLogger(__name__)

class ConsumePubSubStep(BaseStep):
    """Consume messages from Pub/Sub subscription or topic"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from apache_beam.io import ReadFromPubSub
        subscription = self.spec.get("subscription")
        topic = self.spec.get("topic")
        
        if not subscription and not topic:
            raise ValueError(f"Step {self.step_id}: either 'subscription' or 'topic' must be provided")
        
        # with_attributes = self.spec.get("with_attributes", True)
        
        # return PubSubConnector.consume_messages(
        #     pipeline=pipeline,
        #     subscription=subscription,
        #     topic=topic,
        #     with_attributes=with_attributes,
        #     label=self.step_id
        # )
        return pipeline | f"{self.step_id}" >> ReadFromPubSub(
            subscription=subscription,
            topic=topic,
            with_attributes=True,
            id_label='message_id',
            timestamp_attribute='publish_time'
        )

class WindowStep(BaseStep):
    """Apply windowing to streaming data"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        pcoll = self.state[input_key]
        
        # Window configuration
        window_type = self.spec.get("type", "fixed")  # fixed, sliding, session
        size = self.spec.get("size", 60)  # seconds
        
        if window_type == "fixed":
            windowing = window.FixedWindows(size)
        elif window_type == "sliding":
            period = self.spec.get("period", size // 2)
            windowing = window.SlidingWindows(size, period)
        # elif window_type == "session":
        #     gap = self.spec.get("gap", 10)
        #     windowing = window.Sessions(gap)
        else:
            windowing = window.FixedWindows(size)
            # raise ValueError(f"Unknown window type: {window_type}")
        
        # Apply trigger if specified
        trigger_spec = self.spec.get("trigger")
        if trigger_spec:
            trigger = self._build_trigger(trigger_spec)
            return pcoll | f"{self.step_id}_Window" >> beam.WindowInto(
                windowing,
                trigger=trigger,
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
            )
        
        return pcoll | f"{self.step_id}_Window" >> beam.WindowInto(windowing)
    
    def _build_trigger(self, trigger_spec):
        """Build trigger from spec"""
        trigger_type = trigger_spec.get("type", "default")
        
        if trigger_type == "after_watermark":
            early = trigger_spec.get("early_firing_minutes")
            late = trigger_spec.get("late_firing_minutes")
            
            trigger = beam.transforms.trigger.AfterWatermark()
            if early:
                trigger = trigger.with_early_firings(
                    beam.transforms.trigger.AfterProcessingTime(delay=early * 60)
                )
            if late:
                trigger = trigger.with_late_firings(
                    beam.transforms.trigger.AfterCount(late)
                )
            return trigger
        
        return beam.transforms.trigger.Default()

class ExtractKeysStep(BaseStep):
    """Extract keys from Pub/Sub messages for BigTable lookup"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        messages = self.state[input_key]
        # key_field = self.spec.get("key_field", "profiles.Profile.memberId")
        default_path = "profiles.Profile.memberId"
        if hasattr(self.config, 'streaming') and self.config.streaming:
            field_path = self.config.streaming.extract_key.get('field_path', default_path)
        else:
            field_path = self.spec.get("key_field", default_path)
        
        def extract_key(message):
            """Extract key from Pub/Sub message"""
            try:
                # Handle PubsubMessage with attributes
                if hasattr(message, 'data'):
                    data = json.loads(message.data.decode('utf-8'))
                elif isinstance(message, bytes):
                    data = json.loads(message.decode('utf-8'))
                elif isinstance(message, str):
                    data = json.loads(message)
                else:
                    data = message
                
                # Navigate path
                parts = key_field.split('.')
                value = data
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        return None
                
                return value

            except Exception as e:
                LOGGER.warning(f"Failed to extract key: {e}")
                return None
        
        # return (messages 
        #         | f"{self.step_id}_Extract" >> beam.Map(extract_key)
        #         | f"{self.step_id}_FilterNone" >> beam.Filter(lambda x: x is not None))
        return (
            messages 
            | f"{self.step_id}_Extract" >> beam.Map(extract_key)
            | f"{self.step_id}_FilterNone" >> beam.Filter(lambda x: x is not None)
        )


class ReadBigTableRealtimeStep(BaseStep):
    """Read from BigTable using keys"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        keys = self.state[input_key]
        
        # Get BigTable config
        bt_config = self.config.io.get("bigtable", {})
        project = self.spec.get("project") or bt_config.get("project")
        instance = self.spec.get("instance") or bt_config.get("instance")
        table = self.spec.get("table") or bt_config.get("table")
        column_families = self.spec.get("column_families") or bt_config.get("column_families", ["profiles"])
        
        # Cache configuration from spec or default
        cache_config = self.spec.get("cache_config", {
            'hot_cache_size': 1000,
            'hot_cache_ttl': 60,
            'warm_cache_size': 10000,
            'warm_cache_ttl': 300
        })
        
        class ReadFromBigTableWithCache(beam.DoFn):
            """DoFn with caching for BigTable reads"""
            _cache = {}  # Simple in-memory cache
            
            def __init__(self, project_id, instance_id, table_id, column_families, cache_ttl=60):
                self.project_id = project_id
                self.instance_id = instance_id
                self.table_id = table_id
                self.column_families = column_families
                self.cache_ttl = cache_ttl
                self._client = None
                self._table = None
            
            def setup(self):
                from google.cloud import bigtable
                self._client = bigtable.Client(project=self.project_id)
                instance = self._client.instance(self.instance_id)
                self._table = instance.table(self.table_id)
            
            def process(self, member_id):
                import time
                
                # Check cache
                cache_key = f"{self.table_id}:{member_id}"
                if cache_key in self._cache:
                    data, timestamp = self._cache[cache_key]
                    if time.time() - timestamp < self.cache_ttl:
                        yield data
                        return
                
                # Read from BigTable
                row_prefix = f"#1-{member_id}#"
                
                try:
                    rows = self._table.read_rows(
                        row_key_prefix=row_prefix.encode(),
                        limit=1
                    )
                    
                    for row in rows:
                        result = {
                            'member_id': member_id,
                            'found': True,
                            'profiles': {}
                        }
                        
                        # Parse row data
                        for family_id, columns in row.cells.items():
                            family_name = family_id.decode('utf-8')
                            if family_name in self.column_families:
                                for col, cells in columns.items():
                                    col_name = col.decode('utf-8')
                                    value = cells[0].value.decode('utf-8')
                                    
                                    # Try parse JSON
                                    if value and value[0] in ('{', '['):
                                        try:
                                            value = json.loads(value)
                                        except:
                                            pass
                                    
                                    result['profiles'][col_name] = value
                        
                        # Update cache
                        self._cache[cache_key] = (result, time.time())
                        
                        # Manage cache size (simple LRU)
                        if len(self._cache) > cache_config.get('hot_cache_size', 1000):
                            # Remove oldest
                            oldest_key = next(iter(self._cache))
                            del self._cache[oldest_key]
                        
                        yield result
                        return
                    
                    # Not found
                    result = {
                        'member_id': member_id,
                        'found': False,
                        'profiles': {}
                    }
                    self._cache[cache_key] = (result, time.time())
                    yield result
                    
                except Exception as e:
                    LOGGER.error(f"Failed to read {member_id}: {e}")
                    yield {
                        'member_id': member_id,
                        'found': False,
                        'error': str(e),
                        'profiles': {}
                    }

        return keys | f"{self.step_id}_Read" >> beam.ParDo(
            ReadFromBigTableWithCache(
                project,
                instance,
                table,
                column_families,
                cache_config.get('hot_cache_ttl', 60)
            )
        )        
        # return keys | f"{self.step_id}_Read" >> beam.ParDo(
        #     ReadFromBigTable(project, instance, table)
        # )        
        # return BigTableConnector.read_by_keys(
        #     pipeline=keys,  # Pass the keys PCollection
        #     keys=keys,
        #     project_id=project,
        #     instance_id=instance,
        #     table_id=table,
        #     column_family=column_family,
        #     label=self.step_id
        # )



class CreateFixedMappingStep(BaseStep):
    """Create fixed mapping dict for streaming pipeline"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # # Fixed mapping for MS Member streaming
        # mapping_dict = {
        #     "member_id": {
        #         "src_path": ["memberId"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "email": {
        #         "src_path": ["email"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "phone": {
        #         "src_path": ["phone"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "first_name": {
        #         "src_path": ["firstName"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "last_name": {
        #         "src_path": ["lastName"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     # Add more mappings as needed
        # }
        # ดึง mapping จาก config แทน hardcode
        mapping_dict = {}
        
        # Priority: spec > streaming config > default
        if "mapping" in self.spec:
            mapping_dict = self.spec["mapping"]
        elif hasattr(self.config, 'streaming') and self.config.streaming:
            mapping_dict = self.config.streaming.fixed_mapping
        else:
            # Default minimal mapping
            mapping_dict = {
                "member_id": {
                    "src_path": ["member_id"],
                    "reconcile": True,
                    "original": True
                }
            }
        # Return as single element PCollection
        return (
            pipeline
            | f"{self.step_id}_Create" >> beam.Create([mapping_dict])
        )

class CreateEmptyStep(BaseStep):
    """Create empty PCollection for streaming when no reconciliation needed"""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Return empty PCollection with correct KV format
        return (
            pipeline
            | f"{self.step_id}_Empty" >> beam.Create([])
            | f"{self.step_id}_KV" >> beam.Map(lambda x: (None, x))
        )

class WriteToBigQueryStep(BaseStep):
    """Write to BigQuery table"""
    
    def execute(self, pipeline: beam.Pipeline) -> None:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
        
        pcoll = self.state[input_key]
        table = self.spec.get("table")
        
        if not table:
            raise ValueError(f"Step {self.step_id}: 'table' must be provided")
        
        write_disposition = self.spec.get("write_disposition", "WRITE_APPEND")
        create_disposition = self.spec.get("create_disposition", "CREATE_IF_NEEDED")
        
        pcoll | f"{self.step_id}_WriteBQ" >> WriteToBigQuery(
            table=table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            schema="SCHEMA_AUTODETECT"
        )
        
        return None

class ProcessWithDLQStep(BaseStep):
    """Process messages with Dead Letter Queue handling"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from apache_beam.io import WriteToPubSub
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        messages = self.state[input_key]
        dlq_topic = self.spec.get("dlq_topic") or self.config.io.get("pubsub", {}).get("dlq_topic")
        max_retries = self.spec.get("max_retries", 3)
        
        if not dlq_topic:
            # raise ValueError(f"Step {self.step_id}: 'dlq_topic' must be provided")
            return messages
        
        class ProcessWithRetry(beam.DoFn):
            def __init__(self, max_retries):
                self.max_retries = max_retries
            
            def process(self, element):
                # Simple pass-through with error handling
                try:
                    yield beam.pvalue.TaggedOutput('success', element)
                except Exception as e:
                    LOGGER.error(f"Processing failed: {e}")
                    yield beam.pvalue.TaggedOutput('dlq', {
                        'data': element,
                        'error': str(e)
                    })
        
        processed = messages | f"{self.step_id}_Process" >> beam.ParDo(
            ProcessWithRetry(max_retries)
        ).with_outputs('success', 'dlq')
        
        # Send failures to DLQ
        _ = (processed.dlq 
             | f"{self.step_id}_SerializeDLQ" >> beam.Map(json.dumps)
             | f"{self.step_id}_WriteDLQ" >> WriteToPubSub(topic=dlq_topic))
        
        return processed.success
