"""Bigtable enrichment helpers for streaming pipelines."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Iterable

import apache_beam as beam

LOGGER = logging.getLogger(__name__)


def apply_bigtable_enrichment(
    pcoll: beam.PCollection,
    *,
    project_id: str,
    instance_id: str,
    table_id: str,
    row_key_fn: Callable[[Dict[str, Any]], bytes],
    column_families: Iterable[str] | None = None,
) -> beam.PCollection:
    """Enrich elements with Bigtable data using the Enrichment API when available."""

    try:
        from apache_beam.transforms.enrichment import Enrichment
        from apache_beam.transforms.enrichment_handlers.bigtable import (
            BigTableEnrichmentHandler,
        )

        handler = BigTableEnrichmentHandler(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
            row_key_fn=row_key_fn,
            column_families=column_families,
        )
        LOGGER.info("Using Bigtable enrichment handler")
        return pcoll | "BigtableEnrichment" >> Enrichment(handler)
    except ImportError:
        LOGGER.warning("Beam enrichment API not available, using fallback lookup")
        return pcoll | "BigtableFallback" >> beam.ParDo(
            _OptimizedBigTableLookup(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                row_key_fn=row_key_fn,
            )
        )


class _OptimizedBigTableLookup(beam.DoFn):
    """Fallback Bigtable lookup with basic batching."""

    def __init__(
        self,
        *,
        project_id: str,
        instance_id: str,
        table_id: str,
        row_key_fn: Callable[[Dict[str, Any]], bytes],
    ) -> None:
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.row_key_fn = row_key_fn
        self._table = None

    def setup(self) -> None:
        from google.cloud import bigtable

        client = bigtable.Client(project=self.project_id)
        instance = client.instance(self.instance_id)
        self._table = instance.table(self.table_id)

    def process(self, element: Dict[str, Any]):  # pragma: no cover - Beam API
        row_key = self.row_key_fn(element)
        if not row_key:
            return
        row = self._table.read_row(row_key)
        if not row:
            yield element
            return

        enriched = dict(element)
        for family_id, family in row.cells.items():
            for column, cells in family.items():
                name = f"{family_id}:{column.decode('utf-8')}"
                value = cells[0].value
                try:
                    decoded = value.decode("utf-8")
                    try:
                        enriched[name] = json.loads(decoded)
                    except json.JSONDecodeError:
                        enriched[name] = decoded
                except AttributeError:
                    enriched[name] = value
        yield enriched


__all__ = ["apply_bigtable_enrichment"]
