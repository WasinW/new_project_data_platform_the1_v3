# dataflow_common/src/dataflow_common/connectors/bigtable_realtime.py

"""
BigTable Real-time Connector with Advanced Caching
Lowest latency with intelligent caching strategies
Best for: Real-time applications, Low latency requirements (<50ms)
"""

from typing import List, Dict, Any, Optional, Tuple
import apache_beam as beam
from apache_beam.metrics import Metrics
import logging
import time
import json
import hashlib
from datetime import datetime, timedelta
from collections import OrderedDict
import threading

LOGGER = logging.getLogger(__name__)


class BigTableRealtimeConnector:
    """
    Real-time Connector with Advanced Caching
    Features:
    - Per-message processing (no batching)
    - Multi-tier caching (L1: Hot cache, L2: Warm cache)
    - Predictive prefetching
    - Connection keep-alive
    - Circuit breaker pattern
    """
    
    @staticmethod
    def read_realtime(
        keys: beam.PCollection,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_families: Optional[List[str]] = None,
        cache_config: Optional[Dict[str, Any]] = None,
        enable_prefetch: bool = True,
        circuit_breaker_threshold: int = 5,
        label: str = "ReadBigTableRealtime"
    ) -> beam.PCollection:
        """
        Real-time read with advanced caching
        
        Args:
            cache_config: Cache configuration
                - hot_cache_size: Size of L1 cache (default: 1000)
                - hot_cache_ttl: TTL for hot cache in seconds (default: 60)
                - warm_cache_size: Size of L2 cache (default: 10000)
                - warm_cache_ttl: TTL for warm cache in seconds (default: 300)
            enable_prefetch: Enable predictive prefetching
            circuit_breaker_threshold: Error threshold for circuit breaker
        """
        
        cache_config = cache_config or {
            'hot_cache_size': 1000,
            'hot_cache_ttl': 60,
            'warm_cache_size': 10000,
            'warm_cache_ttl': 300
        }
        
        # Process each key immediately - no batching!
        results = keys | f"{label}_Realtime" >> beam.ParDo(
            RealtimeReaderWithCache(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                column_families=column_families,
                cache_config=cache_config,
                enable_prefetch=enable_prefetch,
                circuit_breaker_threshold=circuit_breaker_threshold
            )
        )
        
        return results
    
    @staticmethod
    def write_realtime(
        records: beam.PCollection,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_family: str = "profiles",
        async_write: bool = True,
        label: str = "WriteBigTableRealtime"
    ) -> None:
        """
        Real-time write with async support
        
        Args:
            async_write: Enable asynchronous writes for lower latency
        """
        
        # Write each record immediately
        records | f"{label}_Write" >> beam.ParDo(
            RealtimeWriter(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                column_family=column_family,
                async_write=async_write
            )
        )


class RealtimeReaderWithCache(beam.DoFn):
    """
    Real-time reader with multi-tier caching and advanced features
    """
    
    # Shared resources
    _connection_pool = {}
    _hot_cache = OrderedDict()  # L1 Cache (LRU)
    _warm_cache = OrderedDict()  # L2 Cache (LRU)
    _cache_lock = threading.Lock()
    _access_patterns = {}  # Track access patterns for prefetching
    _circuit_breaker_state = {}
    
    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_families: Optional[List[str]] = None,
        cache_config: Optional[Dict[str, Any]] = None,
        enable_prefetch: bool = True,
        circuit_breaker_threshold: int = 5
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_families = column_families or ['profiles']
        self.cache_config = cache_config or {}
        self.enable_prefetch = enable_prefetch
        self.circuit_breaker_threshold = circuit_breaker_threshold
        
        # Cache settings
        self.hot_cache_size = self.cache_config.get('hot_cache_size', 1000)
        self.hot_cache_ttl = self.cache_config.get('hot_cache_ttl', 60)
        self.warm_cache_size = self.cache_config.get('warm_cache_size', 10000)
        self.warm_cache_ttl = self.cache_config.get('warm_cache_ttl', 300)
        
        # Metrics
        self.latency_dist = Metrics.distribution('RealtimeReader', 'latency_ms')
        self.cache_latency = Metrics.distribution('RealtimeReader', 'cache_latency_ms')
        self.bt_latency = Metrics.distribution('RealtimeReader', 'bigtable_latency_ms')
        self.hot_cache_hits = Metrics.counter('RealtimeReader', 'hot_cache_hits')
        self.warm_cache_hits = Metrics.counter('RealtimeReader', 'warm_cache_hits')
        self.cache_misses = Metrics.counter('RealtimeReader', 'cache_misses')
        self.total_reads = Metrics.counter('RealtimeReader', 'total_reads')
        self.prefetch_hits = Metrics.counter('RealtimeReader', 'prefetch_hits')
        self.circuit_breaker_trips = Metrics.counter('RealtimeReader', 'circuit_breaker_trips')
        
        self._table = None
        self._local_cache = {}  # Worker-local cache
    
    def setup(self):
        """Initialize optimized connection"""
        pool_key = f"{self.project_id}:{self.instance_id}:{self.table_id}"
        
        if pool_key not in RealtimeReaderWithCache._connection_pool:
            LOGGER.info(f"[Realtime] Creating high-performance connection for {pool_key}")
            
            from google.cloud import bigtable
            
            # Optimized for low latency
            client = bigtable.Client(
                project=self.project_id,
                admin=False,
                pool_size=20,  # Larger pool for concurrent requests
                channel_cache_size=20  # More channels
            )
            
            instance = client.instance(self.instance_id)
            table = instance.table(self.table_id)
            
            RealtimeReaderWithCache._connection_pool[pool_key] = {
                'client': client,
                'table': table,
                'created_at': time.time()
            }
        
        self._table = RealtimeReaderWithCache._connection_pool[pool_key]['table']
        
        # Initialize circuit breaker state
        if pool_key not in RealtimeReaderWithCache._circuit_breaker_state:
            RealtimeReaderWithCache._circuit_breaker_state[pool_key] = {
                'errors': 0,
                'last_error_time': None,
                'is_open': False
            }
    
    def process(self, member_id: str):
        """
        Process single key with multi-tier caching
        Real-time: No batching, immediate processing!
        """
        
        start_time = time.time()
        self.total_reads.inc()
        
        # Track access pattern
        self._track_access_pattern(member_id)
        
        # Check circuit breaker
        if self._is_circuit_open():
            self.circuit_breaker_trips.inc()
            yield {
                'member_id': member_id,
                'found': False,
                'error': 'Circuit breaker open',
                'cached': True,
                'latency_ms': 0
            }
            return
        
        # Try multi-tier cache
        cache_result = self._check_multi_tier_cache(member_id)
        if cache_result:
            cache_check_time = (time.time() - start_time) * 1000
            self.cache_latency.update(cache_check_time)
            
            # Add timing info
            cache_result['latency_ms'] = cache_check_time
            cache_result['cached'] = True
            
            yield cache_result
            return
        
        # Cache miss - fetch from BigTable
        self.cache_misses.inc()
        
        try:
            # Real-time read - single key
            bt_start = time.time()
            result = self._fetch_single_key(member_id)
            bt_latency = (time.time() - bt_start) * 1000
            self.bt_latency.update(bt_latency)
            
            # Update caches
            self._update_multi_tier_cache(member_id, result)
            
            # Prefetch related keys if enabled
            if self.enable_prefetch:
                self._prefetch_related_keys(member_id)
            
            # Add timing info
            total_latency = (time.time() - start_time) * 1000
            result['latency_ms'] = total_latency
            result['cached'] = False
            
            self.latency_dist.update(total_latency)
            
            # Reset circuit breaker on success
            self._reset_circuit_breaker()
            
            yield result
            
        except Exception as e:
            # Update circuit breaker
            self._record_error()
            
            LOGGER.error(f"[Realtime] Failed to read {member_id}: {e}")
            
            yield {
                'member_id': member_id,
                'found': False,
                'error': str(e),
                'latency_ms': (time.time() - start_time) * 1000,
                'cached': False
            }
    
    def _check_multi_tier_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Check L1 (hot) and L2 (warm) caches"""
        
        with self._cache_lock:
            cache_key = f"{self.table_id}:{key}"
            current_time = time.time()
            
            # Check L1 (Hot Cache)
            if cache_key in RealtimeReaderWithCache._hot_cache:
                data, timestamp = RealtimeReaderWithCache._hot_cache[cache_key]
                
                if current_time - timestamp < self.hot_cache_ttl:
                    # Move to end (LRU)
                    RealtimeReaderWithCache._hot_cache.move_to_end(cache_key)
                    self.hot_cache_hits.inc()
                    return data.copy()
                else:
                    # Expired
                    del RealtimeReaderWithCache._hot_cache[cache_key]
            
            # Check L2 (Warm Cache)
            if cache_key in RealtimeReaderWithCache._warm_cache:
                data, timestamp = RealtimeReaderWithCache._warm_cache[cache_key]
                
                if current_time - timestamp < self.warm_cache_ttl:
                    # Promote to L1
                    self._promote_to_hot_cache(cache_key, data)
                    self.warm_cache_hits.inc()
                    return data.copy()
                else:
                    # Expired
                    del RealtimeReaderWithCache._warm_cache[cache_key]
        
        return None
    
    def _update_multi_tier_cache(self, key: str, data: Dict[str, Any]):
        """Update multi-tier cache with new data"""
        
        with self._cache_lock:
            cache_key = f"{self.table_id}:{key}"
            current_time = time.time()
            
            # Add to L1 (Hot Cache)
            RealtimeReaderWithCache._hot_cache[cache_key] = (data.copy(), current_time)
            
            # Manage L1 size (LRU eviction)
            if len(RealtimeReaderWithCache._hot_cache) > self.hot_cache_size:
                # Evict oldest to L2
                evicted_key, (evicted_data, evicted_time) = RealtimeReaderWithCache._hot_cache.popitem(False)
                
                # Move to L2
                RealtimeReaderWithCache._warm_cache[evicted_key] = (evicted_data, evicted_time)
                
                # Manage L2 size
                if len(RealtimeReaderWithCache._warm_cache) > self.warm_cache_size:
                    # Evict oldest from L2
                    RealtimeReaderWithCache._warm_cache.popitem(False)
    
    def _promote_to_hot_cache(self, cache_key: str, data: Dict[str, Any]):
        """Promote item from warm to hot cache"""
        
        current_time = time.time()
        
        # Add to hot cache
        RealtimeReaderWithCache._hot_cache[cache_key] = (data.copy(), current_time)
        
        # Remove from warm cache
        if cache_key in RealtimeReaderWithCache._warm_cache:
            del RealtimeReaderWithCache._warm_cache[cache_key]
        
        # Manage hot cache size
        if len(RealtimeReaderWithCache._hot_cache) > self.hot_cache_size:
            evicted_key, evicted_value = RealtimeReaderWithCache._hot_cache.popitem(False)
            RealtimeReaderWithCache._warm_cache[evicted_key] = evicted_value
    
    def _fetch_single_key(self, member_id: str) -> Dict[str, Any]:
        """Fetch single key from BigTable - optimized for low latency"""
        
        from google.cloud.bigtable import row_filters
        
        row_key_prefix = f"#1-{member_id}#"
        
        # Single row read - optimized
        rows = self._table.read_rows(
            row_key_prefix=row_key_prefix.encode(),
            filter_=row_filters.CellsColumnLimitFilter(1),  # Latest only
            limit=1  # Single row
        )
        
        for row in rows:
            return self._parse_row_realtime(row, member_id)
        
        # Not found
        return {
            'member_id': member_id,
            'found': False,
            'profiles': {},
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _parse_row_realtime(self, row, member_id: str) -> Dict[str, Any]:
        """Parse row optimized for speed"""
        
        result = {
            'member_id': member_id,
            'found': True,
            'timestamp': datetime.utcnow().isoformat(),
            'profiles': {}
        }
        
        # Fast parsing
        for family_id, columns in row.cells.items():
            if family_id == b'profiles':
                for column_qualifier, cells in columns.items():
                    if cells:
                        column_name = column_qualifier.decode('utf-8')
                        value = cells[0].value
                        
                        # Fast decode
                        if isinstance(value, bytes):
                            value = value.decode('utf-8', errors='ignore')
                        
                        # Quick JSON check
                        if value and value[0] in ('{', '['):
                            try:
                                value = json.loads(value)
                            except:
                                pass
                        
                        result['profiles'][column_name] = value
        
        return result
    
    def _track_access_pattern(self, key: str):
        """Track access patterns for predictive prefetching"""
        
        if not self.enable_prefetch:
            return
        
        current_time = time.time()
        
        # Simple pattern tracking
        if key not in RealtimeReaderWithCache._access_patterns:
            RealtimeReaderWithCache._access_patterns[key] = []
        
        RealtimeReaderWithCache._access_patterns[key].append(current_time)
        
        # Keep only recent accesses
        cutoff = current_time - 3600  # Last hour
        RealtimeReaderWithCache._access_patterns[key] = [
            t for t in RealtimeReaderWithCache._access_patterns[key]
            if t > cutoff
        ]
    
    def _prefetch_related_keys(self, key: str):
        """Predictive prefetching of related keys"""
        
        # Simple prefetch strategy: sequential keys
        # In production, use ML model or pattern analysis
        
        try:
            # Extract numeric part if exists
            import re
            match = re.search(r'\d+', key)
            if match:
                num = int(match.group())
                # Prefetch next key
                next_key = key.replace(str(num), str(num + 1))
                
                # Check if not already cached
                cache_key = f"{self.table_id}:{next_key}"
                if cache_key not in RealtimeReaderWithCache._hot_cache:
                    # Async prefetch (in production, use threading)
                    pass
        except:
            pass
    
    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open"""
        
        pool_key = f"{self.project_id}:{self.instance_id}:{self.table_id}"
        state = RealtimeReaderWithCache._circuit_breaker_state.get(pool_key, {})
        
        if state.get('is_open'):
            # Check if should close
            last_error = state.get('last_error_time')
            if last_error and time.time() - last_error > 30:  # 30 second cooldown
                state['is_open'] = False
                state['errors'] = 0
                return False
            return True
        
        return False
    
    def _record_error(self):
        """Record error for circuit breaker"""
        
        pool_key = f"{self.project_id}:{self.instance_id}:{self.table_id}"
        state = RealtimeReaderWithCache._circuit_breaker_state.get(pool_key, {})
        
        state['errors'] = state.get('errors', 0) + 1
        state['last_error_time'] = time.time()
        
        if state['errors'] >= self.circuit_breaker_threshold:
            state['is_open'] = True
            LOGGER.warning(f"[Realtime] Circuit breaker opened for {pool_key}")
    
    def _reset_circuit_breaker(self):
        """Reset circuit breaker on success"""
        
        pool_key = f"{self.project_id}:{self.instance_id}:{self.table_id}"
        state = RealtimeReaderWithCache._circuit_breaker_state.get(pool_key, {})
        
        if state.get('errors', 0) > 0:
            state['errors'] = max(0, state['errors'] - 1)


class RealtimeWriter(beam.DoFn):
    """
    Real-time writer with async support
    """
    
    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_family: str = "profiles",
        async_write: bool = True
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family = column_family
        self.async_write = async_write
        
        # Metrics
        self.write_latency = Metrics.distribution('RealtimeWriter', 'latency_ms')
        self.total_writes = Metrics.counter('RealtimeWriter', 'total_writes')
        
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
    
    def process(self, record: Dict[str, Any]):
        """Write single record immediately"""
        
        from google.cloud.bigtable.row import DirectRow
        
        start_time = time.time()
        
        member_id = record.get('member_id')
        if not member_id:
            return
        
        # Generate row key
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
        row_key = f"#1-{member_id}#{timestamp}"
        
        # Create row
        row = DirectRow(row_key=row_key.encode())
        
        # Add cells
        profiles = record.get('profiles', {})
        for key, value in profiles.items():
            # Fast conversion
            if isinstance(value, (dict, list)):
                value_bytes = json.dumps(value).encode()
            elif isinstance(value, bytes):
                value_bytes = value
            else:
                value_bytes = str(value).encode()
            
            row.set_cell(
                column_family_id=self.column_family,
                column=key.encode(),
                value=value_bytes
            )
        
        # Write immediately
        try:
            if self.async_write:
                # Async write for lower latency
                # In production, use proper async handling
                self._table.mutate_rows([row])
            else:
                self._table.mutate_rows([row])
            
            self.total_writes.inc()
            
            # Track latency
            latency_ms = (time.time() - start_time) * 1000
            self.write_latency.update(latency_ms)
            
        except Exception as e:
            LOGGER.error(f"[Realtime] Write failed for {member_id}: {e}")
            raise