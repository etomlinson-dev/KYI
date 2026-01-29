"""
Phase 4: Network Leverage Index (NLI) metrics.
Compute NLI per company per month and store in network_snapshots.
"""
import json
from datetime import datetime
from typing import Dict, Any

from .overlap import compute_overlap_intelligence
from .access_map import load_access_map


def _month_start(dt: datetime) -> str:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0).date().isoformat()


def compute_nli(company_id: int, month: datetime, db) -> Dict[str, Any]:
    """
    Compute and store NLI for company and given month.
    Components:
      - access_gained: unique nodes in access map
      - influence_weighted_overlap: overlap density weighted by relationship_strength (approx via overlap metrics)
      - intro_velocity: intros_sent + meeting_scheduled in month
      - capital_adjacency: count of investor-like titles/orgs in network
    Returns snapshot dict.
    """
    snapshot_month = _month_start(month)

    # Access map metrics
    amap = load_access_map(company_id, db)
    node_count = len(amap.get("nodes", []))
    edge_count = len(amap.get("edges", []))

    # Overlap metrics
    overlap = compute_overlap_intelligence(company_id, db)
    overlap_density = overlap.get("overlap_percentage", 0.0)  # already % of nodes

    # Intro velocity: count intros_sent + meeting_scheduled in this month
    start = snapshot_month
    # naive month end: next month start
    dt = datetime.fromisoformat(snapshot_month)
    if dt.month == 12:
        end_dt = dt.replace(year=dt.year + 1, month=1)
    else:
        end_dt = dt.replace(month=dt.month + 1)
    end = end_dt.date().isoformat()
    rows = db.execute(
        """
        SELECT event_type FROM interactions
        WHERE company_id = ?
          AND event_ts >= ? AND event_ts < ?
        """,
        (company_id, start, end),
    ).fetchall()
    intro_velocity = sum(1 for r in rows if r["event_type"] in ("intro_sent", "meeting_scheduled", "meeting_completed"))

    # Capital adjacency: proxy using node labels (org names and investor-like titles)
    nodes = amap.get("nodes", [])
    capital_like = 0
    from .normalization import extract_firm_type_tokens, matches_title_pattern

    for n in nodes:
        label = (n.get("label") or "").strip()
        if not label:
            continue
        # firm-type tokens or investor-like titles in label
        if extract_firm_type_tokens(label) or matches_title_pattern(label):
            capital_like += 1

    # Combine into NLI 0–100 (simple weighted sum)
    # Normalize components crudely
    access_score = min(1.0, node_count / 500.0)
    overlap_score = overlap_density / 100.0
    intro_score = min(1.0, intro_velocity / 50.0)
    capital_score = min(1.0, capital_like / 100.0)

    nli_score = int(round(
        (0.35 * access_score + 0.25 * overlap_score + 0.2 * intro_score + 0.2 * capital_score) * 100
    ))

    metrics = {
        "total_nodes": node_count,
        "total_edges": edge_count,
        "overlap_density": overlap_density,
        "intro_velocity": intro_velocity,
        "capital_adjacency": capital_like,
        "nli_score": nli_score,
    }

    now = datetime.utcnow().isoformat(timespec="seconds")
    row = db.execute(
        "SELECT id FROM network_snapshots WHERE company_id = ? AND snapshot_month = ?",
        (company_id, snapshot_month),
    ).fetchone()
    if row:
        db.execute(
            "UPDATE network_snapshots SET metrics_json = ?, created_at = ? WHERE id = ?",
            (json.dumps(metrics), now, row["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO network_snapshots (company_id, snapshot_month, metrics_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (company_id, snapshot_month, json.dumps(metrics), now),
        )
    db.commit()
    return metrics


def get_nli_history(company_id: int, db, months: int = 6) -> Dict[str, Any]:
    """
    Return recent NLI snapshots (up to `months` months).
    """
    rows = db.execute(
        """
        SELECT snapshot_month, metrics_json
        FROM network_snapshots
        WHERE company_id = ?
        ORDER BY snapshot_month DESC
        LIMIT ?
        """,
        (company_id, months),
    ).fetchall()
    history = []
    for r in rows:
        try:
            metrics = json.loads(r["metrics_json"])
        except Exception:
            metrics = {}
        history.append({"month": r["snapshot_month"], "metrics": metrics})
    return {"history": history}

