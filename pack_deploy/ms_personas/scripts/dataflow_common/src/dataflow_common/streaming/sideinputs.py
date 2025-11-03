"""Helpers for building cached side inputs in streaming pipelines."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Iterable, Optional

import apache_beam as beam

LOGGER = logging.getLogger(__name__)


class CachedQuerySideInput(beam.DoFn):
    """Generic DoFn that caches the result of a BigQuery query."""

    def __init__(
        self,
        *,
        query: str,
        ttl_seconds: int,
        project_id: Optional[str] = None,
        client_factory: Optional[Callable[..., Any]] = None,
        transform_rows: Optional[Callable[[Iterable[Any]], Any]] = None,
    ) -> None:
        self.query = query
        self.ttl_seconds = ttl_seconds
        self.project_id = project_id
        self.client_factory = client_factory
        self.transform_rows = transform_rows or (lambda rows: list(rows))
        self._client = None
        self._cache = None
        self._cache_time = 0.0

    def setup(self) -> None:
        if self.client_factory is not None:
            self._client = self.client_factory()
        else:
            from google.cloud import bigquery

            self._client = bigquery.Client(project=self.project_id)

    def process(self, element: Any) -> Iterable[Any]:  # pragma: no cover - Beam API
        del element
        now = time.time()
        if self._cache is not None and now - self._cache_time < self.ttl_seconds:
            LOGGER.debug("Returning cached side input result")
            yield self._cache
            return

        LOGGER.info("Refreshing cached side input via query")
        query_job = self._client.query(self.query)
        rows = list(query_job.result())
        LOGGER.debug("Fetched %d rows for cached side input", len(rows))
        transformed = self.transform_rows(rows)
        self._cache = transformed
        self._cache_time = now
        yield transformed


def cached_side_input(
    pipeline: beam.Pipeline,
    loader: CachedQuerySideInput,
    *,
    seed_label: str = "SideInputSeed",
    fetch_label: str = "SideInputFetch",
) -> beam.pvalue.AsSingleton:
    """Return a singleton side input from the provided loader."""

    pcoll = (
        pipeline
        | seed_label >> beam.Create([None])
        | fetch_label >> beam.ParDo(loader)
    )
    return beam.pvalue.AsSingleton(pcoll)


__all__ = ["CachedQuerySideInput", "cached_side_input"]
