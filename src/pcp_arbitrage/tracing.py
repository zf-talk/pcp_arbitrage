"""Pairing summary logs (overwrite per run) and periodic opportunity CSV snapshots."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime, timezone


def write_pairing_log(base_dir: str, exchange: str, body: str) -> None:
    """Overwrite ``{base_dir}/{exchange}.log`` with one pairing report (each process start)."""
    os.makedirs(base_dir, exist_ok=True)
    path = os.path.join(base_dir, f"{exchange}.log")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sep = "=" * 60
    block = f"{sep}\n[{ts}] {exchange}\n{sep}\n{body}"
    if not body.endswith("\n"):
        block += "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(block)


@dataclass
class OpportunitySnap:
    exchange: str
    label: str
    direction: str
    gross: float
    fee: float
    net: float
    ann_pct: float
    qualifies: bool
    updated_ts: float


# (exchange, label, direction) -> latest snap (only when sig is not None)
_snaps: dict[tuple[str, str, str], OpportunitySnap] = {}


def record_opportunity_snap(
    exchange: str,
    label: str,
    direction: str,
    *,
    gross: float,
    fee: float,
    net: float,
    ann_pct: float,
    qualifies: bool,
) -> None:
    key = (exchange, label, direction)
    _snaps[key] = OpportunitySnap(
        exchange=exchange,
        label=label,
        direction=direction,
        gross=gross,
        fee=fee,
        net=net,
        ann_pct=ann_pct,
        qualifies=qualifies,
        updated_ts=__import__("time").time(),
    )


def clear_opportunity_snap(exchange: str, label: str, direction: str) -> None:
    key = (exchange, label, direction)
    _snaps.pop(key, None)


def flush_opportunities_csv(path: str) -> None:
    """Append one snapshot row per tracked opportunity (last computed signal)."""
    if not _snaps:
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    fieldnames = [
        "snapshot_utc",
        "exchange",
        "contract",
        "direction",
        "gross_usdt",
        "fee_usdt",
        "net_usdt",
        "ann_pct",
        "qualifies_min_ann",
    ]
    file_exists = os.path.exists(path) and os.path.getsize(path) > 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for s in _snaps.values():
            w.writerow(
                {
                    "snapshot_utc": iso,
                    "exchange": s.exchange,
                    "contract": s.label,
                    "direction": s.direction,
                    "gross_usdt": f"{s.gross:.6f}",
                    "fee_usdt": f"{s.fee:.6f}",
                    "net_usdt": f"{s.net:.6f}",
                    "ann_pct": f"{s.ann_pct:.6f}",
                    "qualifies_min_ann": str(s.qualifies),
                }
            )
