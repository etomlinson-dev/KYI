"""
Phase 4: Negotiation Intelligence Engine (rules-based MVP).
- Term sheet intake & clause patterns
- Founder-friendliness and control-risk scores
- Investor compare view
"""
import json
from typing import Dict, Any, List

from .normalization import extract_firm_type_tokens, matches_title_pattern


CLAUSE_KEYS = [
    "liquidation_pref",
    "participation",
    "board_seat",
    "protective_provisions",
    "drag_along",
    "pro_rata",
    "redemption",
    "veto_rights",
]


def _load_term_sheets(company_id: int, investor_id: int, db) -> List[Dict[str, Any]]:
    rows = db.execute(
        "SELECT parsed_terms_json FROM term_sheets WHERE company_id = ? AND investor_id = ?",
        (company_id, investor_id),
    ).fetchall()
    out = []
    for r in rows:
        if r["parsed_terms_json"]:
            try:
                out.append(json.loads(r["parsed_terms_json"]))
            except Exception:
                continue
    return out


def _aggregate_clause_stats(term_sheets: List[Dict[str, Any]]) -> Dict[str, Any]:
    freq = {k: 0 for k in CLAUSE_KEYS}
    total = len(term_sheets) or 1
    for ts in term_sheets:
        for k in CLAUSE_KEYS:
            if ts.get(k) not in (None, "", "none", "off"):
                freq[k] += 1
    likelihood = {k: freq[k] / total for k in CLAUSE_KEYS}
    return {"frequency": freq, "likelihood": likelihood}


def _scores_from_clause_stats(stats: Dict[str, Any]) -> (int, int):
    """
    Map clause likelihoods to founder_friendliness and control_risk scores.
    Very simple rules:
      - Heavy control clauses -> higher control_risk, lower founder_friendliness.
      - Light clauses -> opposite.
    """
    like = stats.get("likelihood", {})
    control_weight = 0.0
    econ_weight = 0.0

    # Economic risk: liquidation_pref, participation, redemption
    for k in ("liquidation_pref", "participation", "redemption"):
        p = like.get(k, 0.0)
        econ_weight += p

    # Control risk: board_seat, protective_provisions, veto_rights, drag_along
    for k in ("board_seat", "protective_provisions", "veto_rights", "drag_along"):
        p = like.get(k, 0.0)
        control_weight += p

    # Normalize roughly to 0–1
    econ_risk = min(1.0, econ_weight / 3.0)
    control_risk = min(1.0, control_weight / 4.0)

    control_risk_score = int(round(control_risk * 100))
    founder_friendliness_score = int(round((1.0 - max(econ_risk, control_risk)) * 100))
    return founder_friendliness_score, control_risk_score


def update_investor_clause_patterns(company_id: int, investor_id: int, db) -> Dict[str, Any]:
    """
    Recompute and store clause patterns for an investor, based on term_sheets.
    """
    term_sheets = _load_term_sheets(company_id, investor_id, db)
    stats = _aggregate_clause_stats(term_sheets)
    founder_friendliness, control_risk = _scores_from_clause_stats(stats)

    now = __import__("datetime").datetime.utcnow().isoformat(timespec="seconds")
    row = db.execute(
        "SELECT id FROM investor_clause_patterns WHERE company_id = ? AND investor_id = ?",
        (company_id, investor_id),
    ).fetchone()
    if row:
        db.execute(
            """
            UPDATE investor_clause_patterns
            SET clause_stats_json = ?, founder_friendliness_score = ?, control_risk_score = ?, updated_at = ?
            WHERE id = ?
            """,
            (json.dumps(stats), founder_friendliness, control_risk, now, row["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO investor_clause_patterns
            (company_id, investor_id, clause_stats_json, founder_friendliness_score, control_risk_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (company_id, investor_id, json.dumps(stats), founder_friendliness, control_risk, now),
        )
    db.commit()
    return {
        "clause_stats": stats,
        "founder_friendliness_score": founder_friendliness,
        "control_risk_score": control_risk,
    }


def get_investor_clause_profile(company_id: int, investor_id: int, db) -> Dict[str, Any]:
    row = db.execute(
        """
        SELECT clause_stats_json, founder_friendliness_score, control_risk_score
        FROM investor_clause_patterns
        WHERE company_id = ? AND investor_id = ?
        """,
        (company_id, investor_id),
    ).fetchone()
    if not row:
        return update_investor_clause_patterns(company_id, investor_id, db)
    stats = json.loads(row["clause_stats_json"]) if row["clause_stats_json"] else {}
    return {
        "clause_stats": stats,
        "founder_friendliness_score": row["founder_friendliness_score"],
        "control_risk_score": row["control_risk_score"],
    }


def compare_investors(company_id: int, investor_ids: List[int], db) -> List[Dict[str, Any]]:
    """
    Side-by-side compare investors:
    - behavior axes & metrics are already available
    - clause patterns (founder_friendliness, control_risk)
    (fit_score and relationship_strength summaries can be added from existing modules.)
    """
    from .behavior_profiles import compute_behavior_profile

    results = []
    for inv_id in investor_ids:
        inv = db.execute(
            "SELECT * FROM investors WHERE id = ? AND company_id = ?",
            (inv_id, company_id),
        ).fetchone()
        if not inv:
            continue
        inv = dict(inv)
        behavior = compute_behavior_profile(company_id, inv_id, db)
        clause_profile = get_investor_clause_profile(company_id, inv_id, db)
        results.append({
            "investor_id": inv_id,
            "investor_name": inv["full_name"],
            "behavior_axes": behavior.get("axis_scores", {}),
            "behavior_confidence": behavior.get("confidence", {}),
            "behavior_metrics": behavior.get("behavior_metrics", {}),
            "founder_friendliness_score": clause_profile.get("founder_friendliness_score", 50),
            "control_risk_score": clause_profile.get("control_risk_score", 50),
            "clause_stats": clause_profile.get("clause_stats", {}),
        })
    return results

