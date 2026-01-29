"""
Phase 3: Investor Behavior Intelligence per company.
- Raw behavior metrics from interactions + status history
- Behavioral axes (0–100) + confidence
"""
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
import json


def _parse_ts(ts: str):
    try:
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _episodes_from_interactions(rows) -> Tuple[list, Dict[str, int]]:
    """
    Build decision episodes for one investor:
    from first intro_sent to a decision event (declined / commitment_made / investment_closed).
    Returns (episodes, event_counts) where episode = {start_ts, end_ts, meetings}.
    """
    episodes = []
    event_counts: Dict[str, int] = {}
    current = None
    for r in rows:
        et = r["event_type"]
        event_counts[et] = event_counts.get(et, 0) + 1
        ts = _parse_ts(r["event_ts"])
        if not ts:
            continue
        if et == "intro_sent" and current is None:
            current = {"start_ts": ts, "end_ts": None, "meetings": 0}
        if et == "meeting_completed" and current is not None:
            current["meetings"] += 1
        if et in ("declined", "commitment_made", "investment_closed") and current is not None:
            current["end_ts"] = ts
            episodes.append(current)
            current = None
    return episodes, event_counts


def _behavior_metrics_from_interactions(rows) -> Dict[str, Any]:
    rows = sorted(rows, key=lambda r: r["event_ts"] or "")
    episodes, counts = _episodes_from_interactions(rows)

    # time_to_decision and meetings_to_decision
    tds = []
    mtds = []
    for ep in episodes:
        if ep["start_ts"] and ep["end_ts"]:
            delta_days = (ep["end_ts"] - ep["start_ts"]).total_seconds() / 86400.0
            tds.append(delta_days)
            mtds.append(ep["meetings"])

    avg_time_to_decision_days = sum(tds) / len(tds) if tds else None
    avg_meetings_to_decision = sum(mtds) / len(mtds) if mtds else None

    # response_rate: email_reply / email_sent
    sent = counts.get("email_sent", 0)
    replies = counts.get("email_reply", 0)
    response_rate = (replies / sent) if sent else None

    # followup_latency: placeholder (needs pairwise matching); simple approx: neutral
    followup_latency_hours = None

    # ghosted count
    ghosted = counts.get("ghosted", 0)

    return {
        "avg_time_to_decision_days": avg_time_to_decision_days,
        "avg_meetings_to_decision": avg_meetings_to_decision,
        "response_rate": response_rate,
        "followup_latency_hours": followup_latency_hours,
        "episodes_count": len(episodes),
        "ghosted_count": ghosted,
        "events_count": sum(counts.values()),
    }


def _priority_and_reliability(metrics: Dict[str, Any]) -> Tuple[str, str]:
    """
    Simple rules for priority_style and reliability.
    """
    priority = "unknown"
    reliability = "unknown"
    t = metrics.get("avg_time_to_decision_days")
    m = metrics.get("avg_meetings_to_decision")
    rr = metrics.get("response_rate")
    ghosted = metrics.get("ghosted_count", 0)

    if t is not None and m is not None:
        if t <= 21 and m <= 2:
            priority = "fast_decisive"
        elif t > 30 and m >= 3:
            priority = "slow_deliberate"

    if rr is not None:
        if rr >= 0.6 and ghosted == 0:
            reliability = "high_reliability"
        elif rr < 0.3 or ghosted > 0:
            reliability = "low_reliability"
        else:
            reliability = "moderate_reliability"

    return priority, reliability


def _axis_scores(metrics: Dict[str, Any]) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Rule-based axis scores and confidence.
    All axes in 0–100; confidence in 0–1 based on events/episodes counts.
    """
    episodes = metrics.get("episodes_count", 0) or 0
    events = metrics.get("events_count", 0) or 0
    rr = metrics.get("response_rate")
    t = metrics.get("avg_time_to_decision_days")
    m = metrics.get("avg_meetings_to_decision")
    ghosted = metrics.get("ghosted_count", 0) or 0

    # Confidence: saturating with event count
    def conf_from_count(n: int) -> float:
        if n <= 1:
            return 0.1
        if n <= 3:
            return 0.4
        if n <= 6:
            return 0.7
        return 1.0

    axis_scores: Dict[str, float] = {}
    confidence: Dict[str, float] = {}

    # Risk Appetite: faster decisions & fewer meetings suggest higher risk appetite
    risk = 50.0
    if t is not None and m is not None:
        if t <= 21 and m <= 2:
            risk = 75.0
        elif t > 45 or m >= 4:
            risk = 35.0
    axis_scores["risk_appetite"] = risk
    confidence["risk_appetite"] = conf_from_count(episodes)

    # Control Orientation: more episodes / events implies more control-seeking; we don't have term sheet details yet
    control = 50.0
    if events >= 10:
        control = 65.0
    axis_scores["control_orientation"] = control
    confidence["control_orientation"] = conf_from_count(events)

    # Patience / Time Horizon: longer decision times without ghosting
    patience = 50.0
    if t is not None:
        if t > 45 and ghosted == 0:
            patience = 75.0
        elif t < 14 and ghosted > 0:
            patience = 35.0
    axis_scores["patience"] = patience
    confidence["patience"] = conf_from_count(episodes)

    # Stress Behavior: more ghosted events → lower score
    stress = 70.0
    if ghosted >= 2:
        stress = 40.0
    axis_scores["stress_behavior"] = stress
    confidence["stress_behavior"] = conf_from_count(events)

    # Relationship Style: higher response rate suggests more relationship-oriented
    style = 50.0
    if rr is not None:
        if rr >= 0.7:
            style = 75.0
        elif rr < 0.3:
            style = 35.0
    axis_scores["relationship_style"] = style
    confidence["relationship_style"] = conf_from_count(events)

    # Conviction Strength: faster decisions and low meetings imply strong conviction
    conviction = 50.0
    if t is not None and m is not None:
        if t <= 21 and m <= 2:
            conviction = 75.0
        elif t > 60 and m >= 4:
            conviction = 35.0
    axis_scores["conviction_strength"] = conviction
    confidence["conviction_strength"] = conf_from_count(episodes)

    return axis_scores, confidence


def compute_behavior_profile(company_id: int, investor_id: int, db) -> Dict[str, Any]:
    """
    Compute behavior metrics + axis scores for one investor within a company.
    Stores result in investor_behavior_profiles and returns it.
    """
    rows = db.execute(
        """
        SELECT event_type, event_ts
        FROM interactions
        WHERE company_id = ? AND entity_type = 'investor' AND entity_id = ?
        ORDER BY event_ts ASC
        """,
        (company_id, investor_id),
    ).fetchall()

    metrics = _behavior_metrics_from_interactions(rows)
    priority_style, reliability = _priority_and_reliability(metrics)
    metrics["priority_style"] = priority_style
    metrics["reliability"] = reliability

    axis_scores, confidence = _axis_scores(metrics)

    now = datetime.utcnow().isoformat(timespec="seconds")
    row = db.execute(
        "SELECT id FROM investor_behavior_profiles WHERE investor_id = ? AND company_id = ?",
        (investor_id, company_id),
    ).fetchone()
    if row:
        db.execute(
            """
            UPDATE investor_behavior_profiles
            SET axis_scores = ?, confidence = ?, behavior_metrics = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                json.dumps(axis_scores),
                json.dumps(confidence),
                json.dumps(metrics),
                now,
                row["id"],
            ),
        )
    else:
        db.execute(
            """
            INSERT INTO investor_behavior_profiles
            (investor_id, company_id, axis_scores, confidence, behavior_metrics, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                investor_id,
                company_id,
                json.dumps(axis_scores),
                json.dumps(confidence),
                json.dumps(metrics),
                now,
            ),
        )
    db.commit()

    return {
        "axis_scores": axis_scores,
        "confidence": confidence,
        "behavior_metrics": metrics,
    }

