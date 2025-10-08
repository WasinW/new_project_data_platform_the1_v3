# steps/streaming.py
import apache_beam as beam
from apache_beam.transforms import window
from ..core import BaseStep
from ..connectors.pubsub import PubSubConnector
from ..connectors.bigtable import BigTableConnector
import json
import logging

LOGGER = logging.getLogger(__name__)

class ConsumePubSubStep(BaseStep):
    """Consume messages from Pub/Sub subscription or topic"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        subscription = self.spec.get("subscription")
        topic = self.spec.get("topic")
        
        if not subscription and not topic:
            raise ValueError(f"Step {self.step_id}: either 'subscription' or 'topic' must be provided")
        
        with_attributes = self.spec.get("with_attributes", True)
        
        return PubSubConnector.consume_messages(
            pipeline=pipeline,
            subscription=subscription,
            topic=topic,
            with_attributes=with_attributes,
            label=self.step_id
        )


class ExtractKeysStep(BaseStep):
    """Extract keys from Pub/Sub messages for BigTable lookup"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        messages = self.state[input_key]
        key_field = self.spec.get("key_field", "profiles.Profile.memberId")
        
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

                
                # Navigate path: profiles.Profile.memberId
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
            | f"{self.step_id}_Extract" >> beam.Map(extract_member_id)
            | f"{self.step_id}_FilterNone" >> beam.Filter(lambda x: x is not None)
        )


class ReadBigTableStep(BaseStep):
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
        # column_family = self.spec.get("column_family")
        
        # if not all([project, instance, table]):
        #     raise ValueError(f"Step {self.step_id}: project, instance, and table must be provided")
        class ReadFromBigTable(beam.DoFn):
            def __init__(self, project, instance, table):
                self.project = project
                self.instance = instance
                self.table_name = table
                self._table = None
            
            def setup(self):
                from google.cloud import bigtable
                client = bigtable.Client(project=self.project)
                instance = client.instance(self.instance)
                self._table = instance.table(self.table_name)
            
            def process(self, member_id):
                # Lookup in BigTable
                row_prefix = f"#1-{member_id}#"
                
                try:
                    rows = self._table.read_rows(
                        row_key_prefix=row_prefix.encode(),
                        limit=1
                    )
                    
                    for row in rows:
                        result = {'member_id': member_id}
                        
                        # Get all columns from profiles family
                        if b'profiles' in row.cells:
                            for col, cells in row.cells[b'profiles'].items():
                                col_name = col.decode('utf-8')
                                value = cells[0].value.decode('utf-8')
                                
                                # Try parse JSON if it looks like JSON
                                if value.startswith('{'):
                                    try:
                                        value = json.loads(value)
                                    except:
                                        pass
                                
                                result[col_name] = value
                        
                        yield result
                        return
                    
                    # Not found
                    yield {'member_id': member_id, 'found': False}
                    
                except Exception as e:
                    LOGGER.error(f"Failed to read {member_id}: {e}")
                    yield {'member_id': member_id, 'error': str(e)}
        
        return keys | f"{self.step_id}_Read" >> beam.ParDo(
            ReadFromBigTable(project, instance, table)
        )        
        # return BigTableConnector.read_by_keys(
        #     pipeline=keys,  # Pass the keys PCollection
        #     keys=keys,
        #     project_id=project,
        #     instance_id=instance,
        #     table_id=table,
        #     column_family=column_family,
        #     label=self.step_id
        # )


class ProcessWithDLQStep(BaseStep):
    """Process messages with Dead Letter Queue handling"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        messages = self.state[input_key]
        dlq_topic = self.spec.get("dlq_topic") or self.config.io.get("pubsub", {}).get("dlq_topic")
        max_retries = self.spec.get("max_retries", 3)
        
        if not dlq_topic:
            raise ValueError(f"Step {self.step_id}: 'dlq_topic' must be provided")
        
        # Define processing function
        process_fn_name = self.spec.get("process_function")
        if process_fn_name:
            # Could load custom processing function from registry
            process_fn = PROCESSING_REGISTRY.get(process_fn_name, lambda x: x)
        else:
            # Default pass-through
            process_fn = lambda x: x
        
        results = PubSubConnector.process_with_dlq(
            messages=messages,
            process_fn=process_fn,
            dlq_topic=dlq_topic,
            max_retries=max_retries,
            label=self.step_id
        )
        
        # Store all outputs in state
        self.state[f"{self.step_id}_success"] = results['success']
        self.state[f"{self.step_id}_retry"] = results['retry']
        self.state[f"{self.step_id}_dlq"] = results['dlq']
        
        # Return main output
        return results['success']


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
        elif window_type == "session":
            gap = self.spec.get("gap", 10)
            windowing = window.Sessions(gap)
        else:
            raise ValueError(f"Unknown window type: {window_type}")
        
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