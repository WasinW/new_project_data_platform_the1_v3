"""Utilities for loading and managing mapping definitions in streaming pipelines."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import apache_beam as beam

from dataflow_common.transforms.mapping import create_mapping_dict
from .sideinputs import CachedQuerySideInput, cached_side_input

LOGGER = logging.getLogger(__name__)


@dataclass
class MappingFields:
    """Describes the field names used in the mapping table."""

    src_field: str = "PERSONAS_MAPPING_COLUMN_NAME"
    dest_field: str = "RECONCILE_COLUMN_NAME"
    retrieved_flag_field: str = "RECONCILE_RETRIEVED"
    confirmed_flag_field: str = "RECONCILE_CONFIRMED"


def create_mapping_query(
    project_id: str,
    dataset: str,
    table: str = "stg_mapping_reconcile",
    updated_field: str = "UPDATED_DATE",
    mapping_fields: Optional[MappingFields] = None,
) -> str:
    """Return a query that selects the latest mapping rows."""

    fields = mapping_fields or MappingFields()
    LOGGER.debug(
        "Creating mapping query for %s.%s.%s using updated field %s",
        project_id,
        dataset,
        table,
        updated_field,
    )
    return f"""
    SELECT
      {fields.dest_field},
      {fields.src_field},
      {fields.retrieved_flag_field},
      {fields.confirmed_flag_field},
      {updated_field}
    FROM `{project_id}.{dataset}.{table}`
    WHERE COALESCE({updated_field}, '1999-12-31') = (
      SELECT COALESCE(MAX({updated_field}), '1999-12-31')
      FROM `{project_id}.{dataset}.{table}`
    )
    """


class MappingCacheLoader(CachedQuerySideInput):
    """Load mapping rows from BigQuery and cache the dictionary."""

    def __init__(
        self,
        *,
        project_id: str,
        dataset: str,
        table: str = "stg_mapping_reconcile",
        mapping_fields: Optional[MappingFields] = None,
        ttl_seconds: int = 3600,
        client_factory=None,
        query: Optional[str] = None,
    ) -> None:
        self.mapping_fields = mapping_fields or MappingFields()
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        query = query or create_mapping_query(
            project_id,
            dataset,
            table,
            mapping_fields=self.mapping_fields,
        )
        super().__init__(
            query=query,
            ttl_seconds=ttl_seconds,
            project_id=project_id,
            client_factory=client_factory,
            transform_rows=self._to_mapping,
        )

    def _to_mapping(self, rows: Iterable[Any]) -> Dict[str, Dict[str, Any]]:
        """Convert BigQuery rows into the mapping dictionary."""

        dict_rows = [
            {
                self.mapping_fields.dest_field: row[self.mapping_fields.dest_field],
                self.mapping_fields.src_field: row[self.mapping_fields.src_field],
                self.mapping_fields.retrieved_flag_field: row[
                    self.mapping_fields.retrieved_flag_field
                ],
                self.mapping_fields.confirmed_flag_field: row[
                    self.mapping_fields.confirmed_flag_field
                ],
            }
            for row in rows
        ]
        LOGGER.info("Loaded %d mapping rows", len(dict_rows))
        return create_mapping_dict(
            dict_rows,
            src_field=self.mapping_fields.src_field,
            dest_field=self.mapping_fields.dest_field,
            retrieved_flag_field=self.mapping_fields.retrieved_flag_field,
            confirmed_flag_field=self.mapping_fields.confirmed_flag_field,
        )


def mapping_side_input(
    pipeline: beam.Pipeline,
    loader: MappingCacheLoader,
    label_prefix: str = "Mapping",
) -> beam.pvalue.AsSingleton:
    """Create a singleton side input for the mapping dictionary."""

    return cached_side_input(
        pipeline,
        loader,
        seed_label=f"{label_prefix}Seed",
        fetch_label=f"{label_prefix}Fetch",
    )


__all__ = [
    "MappingFields",
    "create_mapping_query",
    "MappingCacheLoader",
    "mapping_side_input",
]
