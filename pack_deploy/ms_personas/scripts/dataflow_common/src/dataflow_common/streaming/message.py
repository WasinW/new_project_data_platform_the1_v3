"""Message processing primitives for streaming pipelines."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import apache_beam as beam

from dataflow_common.transforms.mapping import map_record

LOGGER = logging.getLogger(__name__)

SUCCESS_TAG = "success"
DLQ_TAG = "dlq"


class ParsePubSubMessage(beam.DoFn):
    """Parse raw Pub/Sub messages into dictionaries with metadata."""

    def __init__(self, *, id_fields: Optional[Iterable[str]] = None) -> None:
        self.id_fields = list(id_fields or ("member_id", "memberId"))

    def process(self, element: Any) -> Iterable[Any]:  # pragma: no cover - Beam API
        try:
            if hasattr(element, "data"):
                data = element.data
                attributes = getattr(element, "attributes", {}) or {}
            else:
                data = element
                attributes = {}

            if isinstance(data, bytes):
                data = data.decode("utf-8")
            if isinstance(data, str):
                payload = json.loads(data)
            else:
                payload = data

            member_id = self._extract_member_id(payload)
            if member_id is None:
                raise ValueError("member_id not found in message")

            result = {
                "member_id": member_id,
                "payload": payload,
                "attributes": attributes,
                "received_at": datetime.now(timezone.utc).isoformat(),
            }
            yield beam.pvalue.TaggedOutput(SUCCESS_TAG, result)
        except Exception as exc:  # pragma: no cover - simple error case
            LOGGER.error("Failed to parse Pub/Sub message: %s", exc)
            dead_letter = {
                "data": self._safe_repr(element),
                "error": str(exc),
                "received_at": datetime.now(timezone.utc).isoformat(),
            }
            yield beam.pvalue.TaggedOutput(DLQ_TAG, dead_letter)

    def _extract_member_id(self, payload: Dict[str, Any]) -> Optional[str]:
        for field in self.id_fields:
            if field in payload:
                return payload[field]

        profiles = payload.get("profiles")
        if isinstance(profiles, str):
            try:
                profiles = json.loads(profiles)
            except json.JSONDecodeError:
                LOGGER.debug("profiles field is not valid JSON")
        if isinstance(profiles, dict):
            for field in self.id_fields:
                if field in profiles:
                    return profiles[field]

        nested = payload.get("payload")
        if isinstance(nested, str):
            try:
                nested = json.loads(nested)
            except json.JSONDecodeError:
                LOGGER.debug("payload field is not valid JSON")
        if isinstance(nested, dict):
            for field in self.id_fields:
                if field in nested:
                    return nested[field]
        return None

    @staticmethod
    def _safe_repr(element: Any) -> str:
        try:
            if hasattr(element, "data"):
                data = element.data
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                return str(data)
            return json.dumps(element)
        except Exception:
            return repr(element)


class ApplyMappingDoFn(beam.DoFn):
    """Apply a mapping dictionary and optional dimension lookup."""

    def __init__(
        self,
        *,
        mode: str = "reconcile",
        pk_field: str = "member_number",
        timestamp_field: str = "_processed_at",
    ) -> None:
        self.mode = mode
        self.pk_field = pk_field
        self.timestamp_field = timestamp_field

    def process(
        self,
        element: Dict[str, Any],
        mapping_dict: Dict[str, Dict[str, Any]],
        dimension_data: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Iterable[Dict[str, Any]]:  # pragma: no cover - Beam API
        payload = element.get("payload", element)
        mapped = map_record(payload, mapping_dict, mode=self.mode)

        primary_key = mapped.get(self.pk_field) or element.get("member_id")
        if primary_key is not None:
            mapped[self.pk_field] = primary_key

        mapped[self.timestamp_field] = datetime.now(timezone.utc).isoformat()

        if dimension_data and primary_key is not None:
            dimension_row = dimension_data.get(str(primary_key))
            if dimension_row:
                mapped.update(dimension_row)

        yield mapped


__all__ = [
    "ParsePubSubMessage",
    "ApplyMappingDoFn",
    "SUCCESS_TAG",
    "DLQ_TAG",
]
