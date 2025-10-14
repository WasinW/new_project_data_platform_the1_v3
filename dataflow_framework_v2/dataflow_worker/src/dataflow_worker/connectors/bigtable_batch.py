# dataflow_common/src/dataflow_common/connectors/bigtable_batch.py

"""
BigTable Optimized Batch Connector
Cost-efficient batch processing with intelligent batching and connection pooling
Best for: Medium traffic (10-1000 msg/sec), Cost optimization
"""

from typing import List, Dict, Any, Optional, Tuple
import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import util
import logging
import time
import json
from datetime import datetime
from collections import defaultdict

LOGGER = logging.getLogger(__name__)


class BigTableBatchConnector:
    """
    Optimized Batch Connector for BigTable
    Features:
    - Intelligent batching (adaptive batch size)
    - Connection pooling
    - Deduplication within batch
    - Basic caching for repeated keys
    - Retry logic with exponential backoff
    """
    
    @staticmethod
    def read_batch(
        keys: beam.PCollection,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_families: Optional[List[str]] = None,
        batch_size: int = 100,
        max_batch_wait_ms: int = 200,
        enable_cache: bool = True,
        cache_ttl_seconds: int = 300,
        max_retries: int = 3,
        label: str = "ReadBigTableBatch"
    ) -> beam.PCollection:
        """
        Optimized batch read from BigTable
        
        Args:
            batch_size: Number of keys to batch (adjust based on traffic)
            max_batch_wait_ms: Max time to wait for batch to fill
            enable_cache: Enable in-memory caching
            cache_ttl_seconds: Cache TTL
            max_retries: Number of retries for failed reads
        """
        
        # Step 1: Clean and deduplicate keys
        clean_keys = (
            keys 
            | f"{label}_FilterEmpty" >> beam.Filter(lambda x: x and str(x).strip())
            | f"{label}_ToString" >> beam.Map(lambda x: str(x).strip())
            | f"{label}_Dedupe" >> beam.Distinct()  # Remove duplicates in window
        )
        
        # Step 2: Adaptive batching based on traffic
        batched = (
            clean_keys
            | f"{label}_Batch" >> util.BatchElements(
                min_batch_size=min(10, batch_size),
                max_batch_size=batch_size,
                max_batch_duration_secs=max_batch_wait_ms / 1000.0
            )
        )
        
        # Step 3: Batch read with optimization
        results = batched | f"{label}_Read" >> beam.ParDo(
            OptimizedBatchReader(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                column_families=column_families,
                enable_cache=enable_cache,
                cache_ttl_seconds=cache_ttl_seconds,
                max_retries=max_retries
            )
        )
        
        return results
    
    @staticmethod
    def write_batch(
        records: beam.PCollection,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_family: str = "profiles",
        batch_size: int = 500,
        max_batch_wait_ms: int = 1000,
        label: str = "WriteBigTableBatch"
    ) -> None:
        """
        Optimized batch write to BigTable
        
        Args:
            batch_size: Number of records to batch for write
            max_batch_wait_ms: Max time to wait before writing
        """
        
        # Batch records for efficient writing
        batched = (
            records
            | f"{label}_FilterValid" >> beam.Filter(
                lambda x: x and x.get('member_id')
            )
            | f"{label}_Batch" >> util.BatchElements(
                min_batch_size=min(100, batch_size),
                max_batch_size=batch_size,
                max_batch_duration_secs=max_batch_wait_ms / 1000.0
            )
        )
        
        # Write batches
        batched | f"{label}_Write" >> beam.ParDo(
            OptimizedBatchWriter(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                column_family=column_family
            )
        )


class OptimizedBatchReader(beam.DoFn):
    """
    Optimized batch reader with intelligent caching and retry logic
    """
    
    # Shared resources across instances
    _connection_pool = {}
    _cache = {}  # Simple LRU cache
    _cache_stats = defaultdict(int)
    
    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_families: Optional[List[str]] = None,
        enable_cache: bool = True,
        cache_ttl_seconds: int = 300,
        max_retries: int = 3
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_families = column_families or ['profiles']
        self.enable_cache = enable_cache
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_retries = max_retries
        
        # Metrics
        self.batch_size_dist = Metrics.distribution('OptimizedBatchReader', 'batch_size')
        self.read_latency = Metrics.distribution('OptimizedBatchReader', 'read_latency_ms')
        self.cache_hit_rate = Metrics.distribution('OptimizedBatchReader', 'cache_hit_rate')
        self.total_reads = Metrics.counter('OptimizedBatchReader', 'total_reads')
        self.cache_hits = Metrics.counter('OptimizedBatchReader', 'cache_hits')
        self.cache_misses = Metrics.counter('OptimizedBatchReader', 'cache_misses')
        self.read_errors = Metrics.counter('OptimizedBatchReader', 'read_errors')
        
        self._table = None
    
    def setup(self):
        """Initialize connection pool"""
        pool_key = f"{self.project_id}:{self.instance_id}:{self.table_id}"
        
        if pool_key not in OptimizedBatchReader._connection_pool:
            LOGGER.info(f"[Batch] Creating connection pool for {pool_key}")
            
            from google.cloud import bigtable
            
            # Optimized client settings for batch
            client = bigtable.Client(
                project=self.project_id,
                admin=False,
                pool_size=10  # Moderate pool size
            )
            
            instance = client.instance(self.instance_id)
            table = instance.table(self.table_id)
            
            OptimizedBatchReader._connection_pool[pool_key] = {
                'client': client,
                'table': table,
                'created_at': time.time()
            }
        
        self._table = OptimizedBatchReader._connection_pool[pool_key]['table']
        
        # Initialize cache management
        self._manage_cache_size()
    
    def process(self, key_batch: List[str]):
        """Process batch of keys efficiently"""
        
        start_time = time.time()
        batch_size = len(key_batch)
        self.batch_size_dist.update(batch_size)
        
        results = []
        keys_to_fetch = []
        
        # Step 1: Check cache for all keys
        if self.enable_cache:
            for key in key_batch:
                cached = self._get_from_cache(key)
                if cached:
                    self.cache_hits.inc()
                    results.append(cached)
                else:
                    self.cache_misses.inc()
                    keys_to_fetch.append(key)
            
            # Calculate cache hit rate
            if batch_size > 0:
                hit_rate = (batch_size - len(keys_to_fetch)) / batch_size * 100
                self.cache_hit_rate.update(hit_rate)
        else:
            keys_to_fetch = key_batch
        
        # Step 2: Batch fetch missing keys
        if keys_to_fetch:
            fetched = self._batch_fetch_with_retry(keys_to_fetch)
            results.extend(fetched)
        
        # Track metrics
        latency_ms = (time.time() - start_time) * 1000
        self.read_latency.update(latency_ms)
        self.total_reads.inc(batch_size)
        
        # Emit results
        for result in results:
            yield result
    
    def _batch_fetch_with_retry(self, keys: List[str]) -> List[Dict[str, Any]]:
        """Fetch batch of keys with retry logic"""
        
        from google.cloud.bigtable import row_set
        from google.cloud.bigtable.row_filters import (
            FamilyNameRegexFilter,
            RowFilterChain,
            CellsColumnLimitFilter
        )
        
        for attempt in range(self.max_retries):
            try:
                # Build optimized row set
                rows_to_read = row_set.RowSet()
                key_map = {}
                
                for member_id in keys:
                    prefix = f"#1-{member_id}#"
                    rows_to_read.add_row_range_from_prefix(prefix.encode())
                    key_map[prefix] = member_id
                
                # Build column filter
                filters = []
                for cf in self.column_families:
                    filters.append(FamilyNameRegexFilter(cf))
                filters.append(CellsColumnLimitFilter(1))  # Latest version only
                
                row_filter = RowFilterChain(filters=filters) if filters else None
                
                # Execute batch read
                rows = self._table.read_rows(
                    row_set=rows_to_read,
                    filter_=row_filter
                )
                
                # Process results
                results = []
                found_members = set()
                
                for row in rows:
                    member_id = self._extract_member_id(row.row_key.decode(), key_map)
                    if member_id:
                        found_members.add(member_id)
                        
                        result = self._parse_row(row, member_id)
                        results.append(result)
                        
                        # Cache result
                        if self.enable_cache:
                            self._put_in_cache(member_id, result)
                
                # Add empty results for not found
                for member_id in keys:
                    if member_id not in found_members:
                        empty_result = {
                            'member_id': member_id,
                            'found': False,
                            'profiles': {},
                            'timestamp': datetime.utcnow().isoformat()
                        }
                        results.append(empty_result)
                        
                        # Cache negative result too
                        if self.enable_cache:
                            self._put_in_cache(member_id, empty_result)
                
                return results
                
            except Exception as e:
                LOGGER.warning(f"[Batch] Attempt {attempt + 1} failed: {e}")
                self.read_errors.inc()
                
                if attempt == self.max_retries - 1:
                    # Final attempt failed, return error results
                    return [{
                        'member_id': key,
                        'found': False,
                        'error': str(e),
                        'profiles': {}
                    } for key in keys]
                
                # Exponential backoff
                time.sleep(0.1 * (2 ** attempt))
    
    def _extract_member_id(self, row_key: str, key_map: Dict[str, str]) -> Optional[str]:
        """Extract member ID from row key"""
        for prefix, member_id in key_map.items():
            if row_key.startswith(prefix):
                return member_id
        
        # Fallback: parse from row key
        parts = row_key.split('#')
        if len(parts) >= 2:
            return parts[1].replace('1-', '')
        return None
    
    def _parse_row(self, row, member_id: str) -> Dict[str, Any]:
        """Parse BigTable row efficiently"""
        
        result = {
            'member_id': member_id,
            'found': True,
            'timestamp': datetime.utcnow().isoformat(),
            'profiles': {}
        }
        
        for family_id, columns in row.cells.items():
            family_name = family_id.decode('utf-8')
            
            if family_name not in self.column_families:
                continue
            
            for column_qualifier, cells in columns.items():
                column_name = column_qualifier.decode('utf-8')
                
                if cells:
                    value = cells[0].value
                    
                    # Decode value
                    if isinstance(value, bytes):
                        try:
                            value = value.decode('utf-8')
                            
                            # Parse JSON if applicable
                            if value.startswith('{') or value.startswith('['):
                                try:
                                    value = json.loads(value)
                                except json.JSONDecodeError:
                                    pass
                        except UnicodeDecodeError:
                            value = value.hex()  # Convert binary to hex
                    
                    if family_name == 'profiles':
                        result['profiles'][column_name] = value
                    else:
                        result[f"{family_name}_{column_name}"] = value
        
        return result
    
    def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get from cache with TTL check"""
        if not self.enable_cache:
            return None
        
        cache_key = f"{self.table_id}:{key}"
        
        if cache_key in OptimizedBatchReader._cache:
            cached_data, cached_time = OptimizedBatchReader._cache[cache_key]
            
            if time.time() - cached_time < self.cache_ttl_seconds:
                return cached_data
            else:
                # Expired
                del OptimizedBatchReader._cache[cache_key]
        
        return None
    
    def _put_in_cache(self, key: str, data: Dict[str, Any]):
        """Store in cache with TTL"""
        if not self.enable_cache:
            return
        
        cache_key = f"{self.table_id}:{key}"
        OptimizedBatchReader._cache[cache_key] = (data, time.time())
    
    def _manage_cache_size(self):
        """Manage cache size with LRU eviction"""
        max_cache_size = 10000
        
        if len(OptimizedBatchReader._cache) > max_cache_size:
            # Remove oldest 20%
            sorted_keys = sorted(
                OptimizedBatchReader._cache.keys(),
                key=lambda k: OptimizedBatchReader._cache[k][1]
            )
            
            for key in sorted_keys[:int(max_cache_size * 0.2)]:
                del OptimizedBatchReader._cache[key]


class OptimizedBatchWriter(beam.DoFn):
    """
    Optimized batch writer for BigTable
    """
    
    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_family: str = "profiles"
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family = column_family
        
        # Metrics
        self.write_batch_size = Metrics.distribution('OptimizedBatchWriter', 'batch_size')
        self.write_latency = Metrics.distribution('OptimizedBatchWriter', 'latency_ms')
        self.total_writes = Metrics.counter('OptimizedBatchWriter', 'total_writes')
        self.write_errors = Metrics.counter('OptimizedBatchWriter', 'errors')
        
        self._table = None
    
    def setup(self):
        """Initialize connection"""
        from google.cloud import bigtable
        
        client = bigtable.Client(
            project=self.project_id,
            admin=False
        )
        instance = client.instance(self.instance_id)
        self._table = instance.table(self.table_id)
    
    def process(self, batch: List[Dict[str, Any]]):
        """Write batch efficiently"""
        
        from google.cloud.bigtable.row import DirectRow
        
        start_time = time.time()
        batch_size = len(batch)
        self.write_batch_size.update(batch_size)
        
        try:
            # Prepare batch mutations
            rows = []
            
            for record in batch:
                member_id = record.get('member_id')
                if not member_id:
                    continue
                
                # Generate row key with timestamp for versioning
                timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
                row_key = f"#1-{member_id}#{timestamp}"
                
                # Create row
                row = DirectRow(row_key=row_key.encode())
                
                # Add cells
                profiles = record.get('profiles', {})
                for key, value in profiles.items():
                    # Convert to bytes
                    if isinstance(value, (dict, list)):
                        value_bytes = json.dumps(value).encode()
                    elif isinstance(value, str):
                        value_bytes = value.encode()
                    elif isinstance(value, bytes):
                        value_bytes = value
                    else:
                        value_bytes = str(value).encode()
                    
                    row.set_cell(
                        column_family_id=self.column_family,
                        column=key.encode(),
                        value=value_bytes
                    )
                
                rows.append(row)
            
            # Batch write
            if rows:
                self._table.mutate_rows(rows)
                self.total_writes.inc(len(rows))
            
            # Track latency
            latency_ms = (time.time() - start_time) * 1000
            self.write_latency.update(latency_ms)
            
            LOGGER.debug(f"[Batch] Wrote {len(rows)} rows in {latency_ms:.2f}ms")
            
        except Exception as e:
            LOGGER.error(f"[Batch] Write failed: {e}")
            self.write_errors.inc(batch_size)
            raise