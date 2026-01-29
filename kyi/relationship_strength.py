"""
Phase 3: Relationship Strength (0–100) between entities (investor, candidate, org).
Rules-based, explainable, and company-scoped.
"""
from datetime import datetime, timezone
import json
from typing import Dict, Any, Optional, Tuple

from .normalization import normalize_name


# Tunable weights (rough; kept simple and documented)
MAX_NETWORK_PTS = 25.0
MAX_INTENSITY_PTS = 35.0
MAX_RECENCY_PTS = 20.0
MAX_PROGRESS_PTS = 20.0


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # Stored as ISO; treat as UTC
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _days_ago(ts: Optional[str]) -> Optional[float]:
    dt = _parse_ts(ts)
    if not dt:
        return None
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    delta = now - dt
    return delta.total_seconds() / 86400.0


def _candidate_key_from_candidate(candidate: Dict[str, Any]) -> str:
    """
    Stable key for candidates (not yet investors):
    normalized_name + '|' + lower(linkedin_url or '').
    """
    name = normalize_name(candidate.get("name") or "")
    url = (candidate.get("linkedin_url") or "").strip().lower()
    return f"{name}|{url}"


def _network_points(shared_investors_count: int, shared_org_count: int, factors: list) -> float:
    pts = 0.0
    if shared_investors_count >= 3:
        pts += 18.0
        factors.append(f"Seen in {shared_investors_count} investor networks (+18)")
    elif shared_investors_count == 2:
        pts += 12.0
        factors.append("Seen in 2 investor networks (+12)")
    elif shared_investors_count == 1:
        pts += 6.0
        factors.append("Seen in 1 investor network (+6)")

    if shared_org_count >= 2:
        pts += 7.0
        factors.append("Common org across networks (+7)")
    elif shared_org_count == 1:
        pts += 4.0
        factors.append("Org appears in your network (+4)")

    return min(pts, MAX_NETWORK_PTS)


def _intensity_points(events: Dict[str, int], factors: list) -> float:
    """
    Interaction intensity from interactions table.
    meetings, replies, documents, decisions.
    """
    pts = 0.0
    meetings = events.get("meeting_completed", 0)
    replies = events.get("email_reply", 0)
    docs = events.get("doc_shared", 0) + events.get("term_sheet_received", 0) + events.get("term_sheet_signed", 0)
    commitments = events.get("commitment_made", 0) + events.get("investment_closed", 0)

    if meetings:
        m_pts = min(20.0, meetings * 6.0)
        pts += m_pts
        factors.append(f"{meetings} meeting(s) completed (+{int(m_pts)})")
    if replies:
        r_pts = min(8.0, replies * 2.0)
        pts += r_pts
        factors.append(f"{replies} reply event(s) (+{int(r_pts)})")
    if docs:
        d_pts = min(5.0, docs * 2.5)
        pts += d_pts
        factors.append(f"{docs} doc/term-sheet event(s) (+{int(d_pts)})")
    if commitments:
        c_pts = min(10.0, commitments * 10.0)
        pts += c_pts
        factors.append(f"{commitments} commitment/closing event(s) (+{int(c_pts)})")

    return min(pts, MAX_INTENSITY_PTS)


def _recency_points(days_since: Optional[float], factors: list) -> float:
    if days_since is None:
        return 0.0
    if days_since <= 7:
        factors.append("Last touch within 7 days (+20)")
        return MAX_RECENCY_PTS
    if days_since <= 30:
        factors.append("Last touch within 30 days (+12)")
        return 12.0
    if days_since <= 90:
        factors.append("Last touch within 90 days (+6)")
        return 6.0
    factors.append("Last touch over 90 days ago (+0)")
    return 0.0


def _progress_points(current_status: Optional[str], factors: list) -> float:
    """
    Map pipeline status to progression depth. Status comes from investor_status_history.
    """
    if not current_status:
        return 0.0
    status = current_status.lower()
    mapping = {
        "prospect": 0.0,
        "contacted": 4.0,
        "meeting": 8.0,
        "interested": 12.0,
        "committed": 16.0,
        "invested": 20.0,
        "inactive": 0.0,
    }
    pts = mapping.get(status, 0.0)
    if pts > 0:
        factors.append(f"Pipeline stage: {status} (+{int(pts)})")
    return min(pts, MAX_PROGRESS_PTS)


def _load_events_for_pair(company_id: int, from_entity: Dict[str, Any], to_entity: Dict[str, Any], db) -> Tuple[Dict[str, int], Optional[str]]:
    """
    Aggregate interactions between two entities (bidirectional), returning:
      - event_counts (by event_type)
      - last_interaction_ts (max event_ts)
    Uses entity_type/entity_id/entity_key to match rows.
    """
    # from_entity and to_entity: {"type": "investor|candidate|org", "id": int or None, "key": str or None}
    etypes = []
    params = [company_id]
    # Match either direction (from <-> to)
    if from_entity["id"] is not None:
        etypes.append("(entity_type = ? AND entity_id = ?)")
        params += [from_entity["type"], from_entity["id"]]
    if from_entity["key"]:
        etypes.append("(entity_type = ? AND entity_key = ?)")
        params += [from_entity["type"], from_entity["key"]]
    if to_entity["id"] is not None:
        etypes.append("(entity_type = ? AND entity_id = ?)")
        params += [to_entity["type"], to_entity["id"]]
    if to_entity["key"]:
        etypes.append("(entity_type = ? AND entity_key = ?)")
        params += [to_entity["type"], to_entity["key"]]

    if not etypes:
        return {}, None

    where_entity = " OR ".join(etypes)
    rows = db.execute(
        f"""
        SELECT event_type, event_ts
        FROM interactions
        WHERE company_id = ?
          AND ({where_entity})
        """,
        tuple(params),
    ).fetchall()

    counts: Dict[str, int] = {}
    last_ts: Optional[str] = None
    for r in rows:
        et = r["event_type"]
        counts[et] = counts.get(et, 0) + 1
        ts = r["event_ts"]
        if not last_ts or (ts and ts > last_ts):
            last_ts = ts
    return counts, last_ts


def _latest_status(company_id: int, entity: Dict[str, Any], db) -> Optional[str]:
    """
    Return latest pipeline status for this entity from investor_status_history.
    """
    clauses = ["company_id = ?", "entity_type = ?"]
    params = [company_id, entity["type"]]
    if entity["id"] is not None:
        clauses.append("entity_id = ?")
        params.append(entity["id"])
    if entity["key"]:
        clauses.append("entity_key = ?")
        params.append(entity["key"])
    where = " AND ".join(clauses)
    row = db.execute(
        f"SELECT status FROM investor_status_history WHERE {where} ORDER BY ts DESC LIMIT 1",
        tuple(params),
    ).fetchone()
    return row["status"] if row else None


def compute_relationship_strength(
    company_id: int,
    from_entity: Dict[str, Any],
    to_entity: Dict[str, Any],
    db,
    shared_investors_count: int = 0,
    shared_org_count: int = 0,
) -> Dict[str, Any]:
    """
    Compute relationship strength 0–100 between two entities and persist to relationships.

    from_entity / to_entity:
      {\"type\": \"investor|candidate|org\", \"id\": int or None, \"key\": str or None}
    """
    factors: list = []

    # 1) Network
    net_pts = _network_points(shared_investors_count, shared_org_count, factors)

    # 2) Interaction intensity + last interaction
    events, last_ts = _load_events_for_pair(company_id, from_entity, to_entity, db)
    intensity_pts = _intensity_points(events, factors)

    # 3) Recency
    days = _days_ago(last_ts)
    rec_pts = _recency_points(days, factors)

    # 4) Progression depth (use 'to_entity' as the counterpart whose status we care about)
    status = _latest_status(company_id, to_entity, db)
    prog_pts = _progress_points(status, factors)

    total_pts = net_pts + intensity_pts + rec_pts + prog_pts
    max_pts = MAX_NETWORK_PTS + MAX_INTENSITY_PTS + MAX_RECENCY_PTS + MAX_PROGRESS_PTS
    strength = int(round(min(100.0, max(0.0, (total_pts / max_pts) * 100.0))))

    # Persist / update relationships row
    now = datetime.utcnow().isoformat(timespec="seconds")
    # Try to find existing row
    row = db.execute(
        """
        SELECT id FROM relationships
        WHERE company_id = ?
          AND from_type = ?
          AND COALESCE(from_id, -1) = COALESCE(?, -1)
          AND COALESCE(from_key, '') = COALESCE(?, '')
          AND to_type = ?
          AND COALESCE(to_id, -1) = COALESCE(?, -1)
          AND COALESCE(to_key, '') = COALESCE(?, '')
        """,
        (
            company_id,
            from_entity["type"],
            from_entity["id"],
            from_entity["key"],
            to_entity["type"],
            to_entity["id"],
            to_entity["key"],
        ),
    ).fetchone()

    factors_json = json.dumps(factors)
    if row:
        db.execute(
            """
            UPDATE relationships
            SET relationship_strength = ?, strength_factors = ?, last_interaction_ts = ?, updated_at = ?
            WHERE id = ?
            """,
            (strength, factors_json, last_ts, now, row["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO relationships
            (company_id, from_type, from_id, from_key, to_type, to_id, to_key,
             relationship_strength, strength_factors, last_interaction_ts, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                company_id,
                from_entity["type"],
                from_entity["id"],
                from_entity["key"],
                to_entity["type"],
                to_entity["id"],
                to_entity["key"],
                strength,
                factors_json,
                last_ts,
                now,
            ),
        )
    db.commit()

    return {
        "relationship_strength": strength,
        "factors": factors,
        "last_interaction_ts": last_ts,
    }


def compute_investor_candidate_strength(
    company_id: int,
    investor_id: int,
    candidate: Dict[str, Any],
    db,
    shared_investors_count: int = 0,
    shared_org_count: int = 0,
) -> Dict[str, Any]:
    """
    Convenience wrapper for investor ↔ candidate relationship.
    candidate: {name, company, position, location, linkedin_url, ...}
    """
    from_entity = {"type": "investor", "id": investor_id, "key": None}
    candidate_key = _candidate_key_from_candidate(candidate)
    to_entity = {"type": "candidate", "id": None, "key": candidate_key}
    return compute_relationship_strength(
        company_id,
        from_entity,
        to_entity,
        db,
        shared_investors_count=shared_investors_count,
        shared_org_count=shared_org_count,
    )

