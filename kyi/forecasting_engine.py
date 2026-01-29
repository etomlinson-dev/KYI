"""
Phase 4: Scenario Forecast Engine (rules-based MVP).
Uses Phase 3 behavior profiles + relationship strength + overlap stats to produce
probabilistic reaction forecasts per investor for a given scenario.
"""
import json
from datetime import datetime
from typing import Dict, Any, List

from .behavior_profiles import compute_behavior_profile
from .relationship_strength import compute_relationship_strength


SCENARIO_TYPES = {
    "missed_revenue",
    "delayed_exit",
    "down_round",
    "choose_between_investors",
    "custom",
}


def _load_investors_for_company(company_id: int, db) -> List[Dict[str, Any]]:
    rows = db.execute(
        "SELECT * FROM investors WHERE company_id = ? ORDER BY full_name",
        (company_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _base_probabilities() -> Dict[str, float]:
    # Supportive, neutral, pressure, control_push, exit_push, ghost
    return {
        "supportive": 0.3,
        "neutral": 0.4,
        "pressure": 0.15,
        "control_push": 0.05,
        "exit_push": 0.05,
        "ghost": 0.05,
    }


def _normalize_probs(probs: Dict[str, float]) -> Dict[str, float]:
    total = sum(probs.values()) or 1.0
    return {k: max(0.0, v / total) for k, v in probs.items()}


def _adjust_for_behavior_and_scenario(
    scenario_type: str,
    axis_scores: Dict[str, float],
    relationship_strength: int,
    probs: Dict[str, float],
    factors: List[str],
) -> None:
    """
    Simple rules:
    - High control_orientation + negative scenario => more control_push
    - Low patience + delayed_exit => more exit_push
    - High risk_appetite + conviction => more supportive in down_round
    - High stress_behavior => more pressure
    - High relationship_strength dampens negative and boosts supportive
    """
    risk = axis_scores.get("risk_appetite", 50)
    control = axis_scores.get("control_orientation", 50)
    patience = axis_scores.get("patience", 50)
    stress = axis_scores.get("stress_behavior", 50)
    conviction = axis_scores.get("conviction_strength", 50)

    # Scenario specific tweaks
    if scenario_type in ("missed_revenue", "down_round"):
        if control >= 60:
            probs["control_push"] += 0.1
            probs["pressure"] += 0.05
            factors.append("High control_orientation in downside scenario (+control_push, +pressure)")
        if stress >= 60:
            probs["pressure"] += 0.1
            factors.append("High stress_behavior in negative scenario (+pressure)")
        if conviction >= 65 and risk >= 60:
            probs["supportive"] += 0.1
            factors.append("High conviction & risk appetite (+supportive)")
    if scenario_type == "delayed_exit":
        if patience < 45:
            probs["exit_push"] += 0.1
            probs["pressure"] += 0.05
            factors.append("Low patience in delayed exit scenario (+exit_push, +pressure)")
        else:
            probs["neutral"] += 0.05
            factors.append("Higher patience dampens negative reactions (+neutral)")

    # Relationship strength dampening
    if relationship_strength >= 70:
        # Reduce negative, boost supportive
        probs["supportive"] += 0.1
        probs["pressure"] *= 0.7
        probs["control_push"] *= 0.7
        probs["exit_push"] *= 0.7
        factors.append("Strong relationship reduces negative reactions (+supportive)")


def _confidence_from_profile_and_data(axis_conf: Dict[str, float], behavior_metrics: Dict[str, Any], has_term_sheet: bool) -> float:
    """
    Confidence 0–1 based on:
    - average axis confidence
    - episodes/events count
    - presence of term sheet patterns
    """
    avg_axis_conf = sum(axis_conf.values()) / max(len(axis_conf), 1) if axis_conf else 0.2
    episodes = behavior_metrics.get("episodes_count", 0) or 0
    events = behavior_metrics.get("events_count", 0) or 0
    base = avg_axis_conf
    if episodes >= 2:
        base += 0.1
    if events >= 10:
        base += 0.1
    if has_term_sheet:
        base += 0.1
    return max(0.1, min(1.0, base))


def run_scenario(company_id: int, scenario_row: Dict[str, Any], db) -> Dict[str, Any]:
    """
    Run a scenario for a company, returning and persisting results:
    - Per-investor forecast with probabilities, confidence, and factors.
    - Aggregated guidance text.
    Stores a row in scenario_runs.
    """
    scenario_type = scenario_row.get("scenario_type")
    if scenario_type not in SCENARIO_TYPES:
        scenario_type = "custom"

    investors = _load_investors_for_company(company_id, db)
    if not investors:
        result = {"investors": [], "guidance": [], "scenario_type": scenario_type}
        _store_scenario_run(company_id, scenario_row["id"], result, 0.1, db)
        return result

    per_investor = []
    confidences = []
    for inv in investors:
        # Behavior profile for this investor
        profile = compute_behavior_profile(company_id, inv["id"], db)
        axes = profile.get("axis_scores", {})
        axis_conf = profile.get("confidence", {})
        metrics = profile.get("behavior_metrics", {})

        # Relationship strength between this investor and the company context (treated as org)
        from_entity = {"type": "investor", "id": inv["id"], "key": None}
        to_entity = {"type": "org", "id": None, "key": f"company:{company_id}"}
        rel = compute_relationship_strength(company_id, from_entity, to_entity, db, 0, 0)
        relationship_strength = rel.get("relationship_strength", 0)

        probs = _base_probabilities()
        factors: List[str] = []
        _adjust_for_behavior_and_scenario(scenario_type, axes, relationship_strength, probs, factors)
        probs = _normalize_probs(probs)

        # Confidence
        has_term_sheet = bool(
            db.execute(
                "SELECT 1 FROM term_sheets WHERE company_id = ? AND investor_id = ? LIMIT 1",
                (company_id, inv["id"]),
            ).fetchone()
        )
        conf = _confidence_from_profile_and_data(axis_conf, metrics, has_term_sheet)
        confidences.append(conf)

        per_investor.append({
            "investor_id": inv["id"],
            "investor_name": inv["full_name"],
            "probabilities": probs,
            "relationship_strength": relationship_strength,
            "behavior_axes": axes,
            "confidence": conf,
            "factors": factors[:6],
        })

    # Aggregate guidance (simple textual hints)
    guidance = []
    # Identify high control_push risk investors
    risky = [r for r in per_investor if r["probabilities"]["control_push"] >= 0.2]
    if risky:
        names = ", ".join(r["investor_name"] for r in risky[:5])
        guidance.append(f"Investors likely to push for control terms: {names}.")
    # Identify supportive / high conviction
    supportive = [r for r in per_investor if r["probabilities"]["supportive"] >= 0.4]
    if supportive:
        names = ", ".join(r["investor_name"] for r in supportive[:5])
        guidance.append(f"Most supportive profiles in this scenario: {names}.")

    avg_conf = sum(confidences) / max(len(confidences), 1)
    result = {
        "scenario_type": scenario_type,
        "investors": per_investor,
        "guidance": guidance,
    }
    _store_scenario_run(company_id, scenario_row["id"], result, avg_conf, db)
    return result


def _store_scenario_run(company_id: int, scenario_id: int, result: Dict[str, Any], confidence: float, db) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    db.execute(
        """
        INSERT INTO scenario_runs (scenario_id, company_id, run_ts, results_json, confidence_score, model_version)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            scenario_id,
            company_id,
            now,
            json.dumps(result),
            confidence,
            "rules_v1",
        ),
    )
    db.commit()

