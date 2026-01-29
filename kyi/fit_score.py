"""
Phase 2: Potential Investor Rating (Fit Score) 0–100 + explainable factors.
Tunable weights; no hard-coded sample data.
"""
from .normalization import (
    tokenize_location,
    tokenize_industry,
    extract_firm_type_tokens,
    matches_title_pattern,
    FIRM_TYPE_TOKENS,
)

# --- Tuning: weights for fit score dimensions (sum to 1.0 or scale to 0–100) ---
WEIGHT_SIMILARITY = 0.30   # industry + title + firm-type overlap
WEIGHT_NETWORK = 0.35      # shared_investors_count, shared orgs
WEIGHT_LOCATION = 0.20     # location overlap
WEIGHT_RECENCY = 0.15      # recency (neutral if no data)
MAX_SIMILARITY_PTS = 30
MAX_NETWORK_PTS = 35
MAX_LOCATION_PTS = 20
MAX_RECENCY_PTS = 15


def _industry_overlap_score(candidate_title: str, candidate_company: str, profile_industry_tokens: set) -> float:
    """0–10 pts: candidate title/company overlap with investor industry tokens."""
    if not profile_industry_tokens:
        return 0.0
    text = f" {candidate_title or ''} {candidate_company or ''} ".lower()
    matches = sum(1 for t in profile_industry_tokens if t in text)
    if not matches:
        return 0.0
    return min(10.0, 3.0 + matches * 2.0)


def _title_pattern_score(candidate_title: str) -> float:
    """0–10 pts: candidate has investor-like title."""
    if not candidate_title:
        return 0.0
    return 10.0 if matches_title_pattern(candidate_title) else 0.0


def _firm_type_score(candidate_company: str, profile_firm_tokens: set) -> float:
    """0–10 pts: candidate company looks like investor org or similar to existing firms."""
    if not candidate_company:
        return 0.0
    cl = candidate_company.strip().lower()
    pts = 0.0
    for ft in FIRM_TYPE_TOKENS:
        if ft in cl:
            pts += 5.0
            break
    if profile_firm_tokens and any(f in cl or cl in f for f in profile_firm_tokens if len(f) > 4):
        pts += 5.0
    return min(10.0, pts)


def _similarity_score(candidate: dict, company_profile: dict) -> tuple:
    """Returns (points 0–MAX_SIMILARITY_PTS, factor_list)."""
    industry_tokens = company_profile.get("industry_tokens") or set()
    firm_type_tokens = company_profile.get("investor_firms_lower") or set()
    title = (candidate.get("position") or "").strip()
    company = (candidate.get("company") or "").strip()

    ind_pts = _industry_overlap_score(title, company, industry_tokens)
    title_pts = _title_pattern_score(title)
    firm_pts = _firm_type_score(company, firm_type_tokens)
    raw = ind_pts + title_pts + firm_pts  # 0–30
    scale = MAX_SIMILARITY_PTS / 30.0 if raw else 0
    pts = raw * scale
    factors = []
    if ind_pts > 0:
        factors.append("Industry overlap with your investors")
    if title_pts > 0:
        factors.append("Investor-like title")
    if firm_pts > 0:
        factors.append("Firm type / similar to your investors")
    return pts, factors


def _network_score(shared_investors_count: int, shared_org_count: int) -> tuple:
    """Returns (points 0–MAX_NETWORK_PTS, factor_list)."""
    pts = 0.0
    factors = []
    # Shared investors: up to 20 pts (e.g. 1=4, 2=8, 3+=20)
    if shared_investors_count >= 3:
        pts += 20.0
        factors.append(f"Seen in {shared_investors_count} investor networks")
    elif shared_investors_count == 2:
        pts += 12.0
        factors.append("Seen in 2 investor networks")
    elif shared_investors_count == 1:
        pts += 5.0
        factors.append("In 1 investor's network")
    # Shared org: up to 15 pts
    if shared_org_count >= 2:
        pts += 15.0
        factors.append("Company appears across network")
    elif shared_org_count == 1:
        pts += 7.0
        factors.append("Company in network")
    pts = min(pts, MAX_NETWORK_PTS)
    return pts, factors


def _location_score(candidate_location: str, profile_location_tokens: set) -> tuple:
    """Returns (points 0–MAX_LOCATION_PTS, factor_list)."""
    if not candidate_location or not candidate_location.strip():
        return 0.0, []
    if not profile_location_tokens:
        return 0.0, []
    loc_tokens = tokenize_location(candidate_location)
    overlap = loc_tokens & profile_location_tokens
    if not overlap:
        return 0.0, []
    return MAX_LOCATION_PTS, ["Location match with your investors"]


def _recency_score(_candidate: dict) -> tuple:
    """Returns (points, factor_list). No interaction timestamps yet; keep neutral."""
    return MAX_RECENCY_PTS * 0.5, []  # neutral 50% of max


def compute_fit_score(
    company_id: int,
    candidate: dict,
    company_profile: dict,
    shared_investors_count: int = 0,
    shared_org_count: int = 0,
) -> dict:
    """
    Compute fit score 0–100 and factor breakdown for a candidate.
    candidate: { name, company, position, location, linkedin_url, ... }
    company_profile: from build_company_profile(investors)
    shared_investors_count: number of company investors whose connection list contains this candidate
    shared_org_count: 1 if candidate_company appears in network, else 0 (or count)
    Returns: { fit_score (0–100), factors (list of str), breakdown (dict per dimension) }
    """
    sim_pts, sim_factors = _similarity_score(candidate, company_profile)
    net_pts, net_factors = _network_score(shared_investors_count, shared_org_count)
    loc_pts, loc_factors = _location_score(
        candidate.get("location") or "",
        company_profile.get("location_tokens") or set(),
    )
    rec_pts, rec_factors = _recency_score(candidate)

    total = sim_pts + net_pts + loc_pts + rec_pts
    # Scale to 0–100
    max_pts = MAX_SIMILARITY_PTS + MAX_NETWORK_PTS + MAX_LOCATION_PTS + MAX_RECENCY_PTS
    fit_score = round(min(100, max(0, (total / max_pts) * 100)))

    factors = sim_factors + net_factors + loc_factors + rec_factors
    if len(factors) > 6:
        factors = factors[:6]

    breakdown = {
        "similarity": round(sim_pts, 1),
        "network": round(net_pts, 1),
        "location": round(loc_pts, 1),
        "recency": round(rec_pts, 1),
    }
    return {
        "fit_score": fit_score,
        "factors": factors,
        "breakdown": breakdown,
    }
