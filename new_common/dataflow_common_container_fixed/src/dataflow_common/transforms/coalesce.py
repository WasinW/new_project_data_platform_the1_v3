"""
Coalesce helpers for the dataflow_common package.

When new and old records have been joined by key the
:func:`coalesce_by_mapping` function decides which value to keep for
each destination column based on a flag in the mapping rows.  If the
preferred side has ``None`` for a given column then the value from
the other side is used.  The primary key is always preserved even if
it is missing from one side.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple


def coalesce_by_mapping(
    kv: Tuple[Any, Dict[str, List[Dict[str, Any]]]],
    *,
    columns: Iterable[Dict[str, Any]],
    flag_field: str,
    pk_field: str,
    dest_field: str = "dest_column_name",
) -> Dict[str, Any]:
    """Coalesce values from new/old rows based on a mapping.

    Parameters
    ----------
    kv: tuple
        A tuple ``(key, groups)`` produced by
        ``beam.CoGroupByKey`` where ``groups`` is a dictionary
        mapping alias names to lists of records.  Typically aliases
        ``'new'`` and ``'old'`` are used.
    columns: iterable of dict
        The mapping rows used to determine which flag field applies to
        each destination column.  Each row must contain the
        destination column name under ``dest_field`` and the
        coalescing flag under ``flag_field``.
    flag_field: str
        Name of the field in ``columns`` that determines whether to
        prefer the new row (flag is truthy) or the old row (flag is
        falsey).
    pk_field: str
        Name of the primary key field that must always be present in
        the output.  The value is taken from the new row if present
        otherwise from the old row.
    dest_field: str, optional
        Name of the field in ``columns`` containing the destination
        column name.  Defaults to ``'RECONCILE_COLUMN_NAME'``.

    Returns
    -------
    dict
        A dictionary containing the coalesced values for each
        destination column and the primary key.
    """
    key, groups = kv
    new_rows = groups.get("new") or []
    old_rows = groups.get("old") or []
    new_row: Dict[str, Any] = new_rows[0] if new_rows else {}
    old_row: Dict[str, Any] = old_rows[0] if old_rows else {}
    out: Dict[str, Any] = {}
    for row in columns:
        if not row:
            continue
        tgt = row.get(dest_field)
        if not tgt:
            continue
        prefer_new = bool(row.get(flag_field))
        preferred = new_row.get(tgt) if prefer_new else old_row.get(tgt)
        if preferred is None:
            fallback = old_row.get(tgt) if prefer_new else new_row.get(tgt)
            out[tgt] = fallback
        else:
            out[tgt] = preferred
    # ensure PK always present
    if pk_field in new_row:
        out.setdefault(pk_field, new_row.get(pk_field))
    if pk_field in old_row:
        out.setdefault(pk_field, old_row.get(pk_field))
    return out


__all__ = ["coalesce_by_mapping"]