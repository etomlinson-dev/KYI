"""
KYI recommendation pipeline: company-scoped suggested investors.
- Multi-signal gating: require >= 2 independent signal categories.
- De-duplication: exclude existing investors (normalized name, linkedin_url, fuzzy name).
- Explainability: each candidate has signals{} and reasons[].
"""
from difflib import SequenceMatcher

from .normalization import (
    normalize_name,
    tokenize_location,
    tokenize_industry,
    extract_firm_type_tokens,
    matches_title_pattern,
    FIRM_TYPE_TOKENS,
)

# --- Signal category keys (used for gating: count unique categories) ---
S_INDUSTRY = "s_industry"
S_LOCATION = "s_location"
S_FIRM_TYPE = "s_firm_type"
S_TITLE_PATTERN = "s_title_pattern"
S_COMPANY_IN_NETWORK = "s_company_in_network"

SIGNAL_CATEGORIES = (S_INDUSTRY, S_LOCATION, S_FIRM_TYPE, S_TITLE_PATTERN, S_COMPANY_IN_NETWORK)

# --- Tuning: change here to adjust gating, dedup, and feed size ---
MIN_SIGNAL_CATEGORIES = 2  # Multi-signal gate: keep only if unique_signal_categories >= this
FUZZY_NAME_THRESHOLD = 0.88  # Dedup: exclude candidate if (company+title) match and name similarity >= this
DEFAULT_TOP_N = 100  # Max number of suggested investors returned per company


def load_company_investors(company_id: int, db) -> list:
    """Load investors belonging to the given company. Returns list of dict-like rows."""
    rows = db.execute(
        "SELECT * FROM investors WHERE company_id = ? ORDER BY full_name",
        (company_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def load_connections_for_investors(investor_ids: list, db) -> list:
    """Load all connections for the given investor IDs. Returns list of connection dicts with investor_id."""
    if not investor_ids:
        return []
    placeholders = ",".join("?" * len(investor_ids))
    rows = db.execute(
        f"""
        SELECT c.*, c.investor_id AS source_investor_id
        FROM connections c
        WHERE c.investor_id IN ({placeholders})
        AND (c.full_name IS NOT NULL AND c.full_name != '')
        """,
        tuple(investor_ids),
    ).fetchall()
    return [dict(r) for r in rows]


def build_company_profile(investors: list) -> dict:
    """
    Build company profile from investors: industry tokens, location tokens,
    firm-type tokens, title patterns, and company-in-network counts (from their connections).
    Caller can optionally pass connections to compute company_in_network; we do that in run_pipeline.
    """
    industry_tokens = set()
    location_tokens = set()
    firm_type_tokens = set()
    title_patterns = set()  # we use matches_title_pattern on candidate; this is from investors for display

    for inv in investors:
        if inv.get("industry"):
            industry_tokens |= tokenize_industry(inv["industry"])
        if inv.get("location") and str(inv["location"]).strip():
            location_tokens |= tokenize_location(inv["location"])
        if inv.get("firm"):
            firm_type_tokens |= extract_firm_type_tokens(inv["firm"])
        if inv.get("title"):
            title_patterns.add(inv["title"].strip().lower())

    return {
        "industry_tokens": industry_tokens,
        "location_tokens": location_tokens,
        "firm_type_tokens": firm_type_tokens,
        "title_patterns": title_patterns,
        "investor_firms_lower": {inv.get("firm", "").strip().lower() for inv in investors if inv.get("firm")},
    }


def _connection_company_counts(connections: list) -> dict:
    """Map company (lowercase) -> count of connections at that company."""
    counts = {}
    for c in connections:
        comp = (c.get("company") or "").strip()
        if not comp:
            continue
        key = comp.lower()
        counts[key] = counts.get(key, 0) + 1
    return counts


def score_candidates(connections: list, company_profile: dict, company_connection_counts: dict) -> list:
    """
    Score each connection as a candidate. Returns list of:
    { candidate (dict), score (float), signals (dict category -> True), reasons (list of str) }
    signals: only keys that fired (so we can count unique categories).
    """
    industry_tokens = company_profile.get("industry_tokens") or set()
    location_tokens = company_profile.get("location_tokens") or set()
    firm_type_tokens = company_profile.get("firm_type_tokens") or set()
    investor_firms_lower = company_profile.get("investor_firms_lower") or set()

    # company_in_network: companies that appear in 2+ connections (common in network)
    common_companies = {k for k, v in company_connection_counts.items() if v >= 2}

    results = []
    for conn in connections:
        full_name = (conn.get("full_name") or "").strip()
        if not full_name:
            full_name = f"{conn.get('first_name') or ''} {conn.get('last_name') or ''}".strip()
        if not full_name:
            continue

        company = (conn.get("company") or "").strip()
        position = (conn.get("position") or "").strip()
        location = (conn.get("location") or "").strip()
        linkedin_url = (conn.get("linkedin_url") or "").strip()

        candidate = {
            "name": full_name,
            "company": company,
            "position": position,
            "location": location,
            "linkedin_url": linkedin_url,
            "source_investor_id": conn.get("source_investor_id"),
        }

        signals = {}
        reasons = []
        score = 0.0

        # s_industry: candidate title/company contains investor industry tokens
        text = f" {position} {company} ".lower()
        for tok in industry_tokens:
            if tok in text:
                signals[S_INDUSTRY] = True
                reasons.append(f"Industry: {tok}")
                score += 4.0
                break

        # s_location: candidate location overlaps with investor/company location tokens
        if location and location_tokens:
            loc_tokens = tokenize_location(location)
            if loc_tokens & location_tokens:
                signals[S_LOCATION] = True
                reasons.append(f"Location match")
                score += 3.0

        # s_firm_type: candidate company looks like investor org
        if company:
            cl = company.lower()
            for ft in FIRM_TYPE_TOKENS:
                if ft in cl:
                    signals[S_FIRM_TYPE] = True
                    reasons.append(f"Firm type: {ft}")
                    score += 3.0
                    break
            # Similar to existing investor firm names
            for firm in investor_firms_lower:
                if len(firm) > 4 and (firm in cl or cl in firm):
                    if S_FIRM_TYPE not in signals:
                        signals[S_FIRM_TYPE] = True
                        reasons.append(f"Similar to firm")
                    score += 2.0
                    break

        # s_title_pattern: candidate title matches investor-like role
        if position and matches_title_pattern(position):
            signals[S_TITLE_PATTERN] = True
            reasons.append("Investor-like title")
            score += 3.0

        # s_company_in_network: candidate's company appears across multiple connections
        if company:
            key = company.lower()
            if key in common_companies:
                signals[S_COMPANY_IN_NETWORK] = True
                cnt = company_connection_counts.get(key, 0)
                reasons.append(f"Company in network ({cnt} connections)")
                score += 5.0

        results.append({
            "candidate": candidate,
            "score": score,
            "signals": signals,
            "reasons": reasons,
        })

    return results


def apply_multi_signal_gate(candidates: list, min_categories: int = MIN_SIGNAL_CATEGORIES) -> list:
    """Keep only candidates with >= min_categories unique signal categories."""
    return [c for c in candidates if len(c.get("signals") or {}) >= min_categories]


def _build_existing_indexes(existing_investors: list) -> dict:
    """Build normalized_name set, linkedin_url set, and (company, title) -> list of (name, normalized_name)."""
    normalized_names = set()
    linkedin_urls = set()
    company_title_to_investors = {}  # (company_lower, title_lower) -> [(full_name, normalized_name), ...]

    for inv in existing_investors:
        name = (inv.get("full_name") or "").strip()
        if name:
            normalized_names.add(normalize_name(name))
        url = (inv.get("linkedin_url") or "").strip()
        if url:
            linkedin_urls.add(url.lower().strip())
        firm = (inv.get("firm") or "").strip().lower()
        title = (inv.get("title") or "").strip().lower()
        if firm or title:
            key = (firm, title)
            if key not in company_title_to_investors:
                company_title_to_investors[key] = []
            company_title_to_investors[key].append((name, normalize_name(name)))

    return {
        "normalized_names": normalized_names,
        "linkedin_urls": linkedin_urls,
        "company_title_to_investors": company_title_to_investors,
    }


def _fuzzy_match(name: str, existing_names_normalized: list, threshold: float) -> bool:
    """True if name (normalized) matches any existing by similarity >= threshold."""
    n = normalize_name(name)
    for ex in existing_names_normalized:
        if SequenceMatcher(None, n, ex).ratio() >= threshold:
            return True
    return False


def apply_dedup(
    candidates: list,
    existing_investors: list,
    fuzzy_threshold: float = FUZZY_NAME_THRESHOLD,
) -> list:
    """
    Remove candidates that match an existing investor by:
    - linkedin_url exact match (if present), OR
    - normalized_name match, OR
    - (company + title) match AND fuzzy name similarity >= fuzzy_threshold
    """
    indexes = _build_existing_indexes(existing_investors)
    normalized_names = indexes["normalized_names"]
    linkedin_urls = indexes["linkedin_urls"]
    company_title_to_investors = indexes["company_title_to_investors"]

    deduped = []
    for item in candidates:
        c = item["candidate"]
        name = c.get("name") or ""
        url = (c.get("linkedin_url") or "").strip().lower()
        company_lower = (c.get("company") or "").strip().lower()
        title_lower = (c.get("position") or "").strip().lower()
        key = (company_lower, title_lower)

        # 1) LinkedIn URL match
        if url and url in linkedin_urls:
            continue
        # 2) Normalized name match
        if name and normalize_name(name) in normalized_names:
            continue
        # 3) (company + title) match and fuzzy name
        if key in company_title_to_investors:
            existing_list = company_title_to_investors[key]
            existing_norm = [n for _, n in existing_list]
            if _fuzzy_match(name, existing_norm, fuzzy_threshold):
                continue

        deduped.append(item)

    return deduped


def run_pipeline(
    company_id: int,
    db,
    top_n: int = DEFAULT_TOP_N,
    min_signal_categories: int = MIN_SIGNAL_CATEGORIES,
    fuzzy_threshold: float = FUZZY_NAME_THRESHOLD,
) -> tuple:
    """
    Full pipeline for one company:
    1. load_company_investors
    2. load_connections_for_investors
    3. build_company_profile
    4. score_candidates (with company_connection_counts from connections)
    5. merge by candidate identity (normalized name + linkedin_url), keep max score and combined reasons/signals
    6. apply_multi_signal_gate
    7. apply_dedup
    8. sort by score desc, return top_n

    Returns (suggested_list, company_name, investor_count, connection_count).
    suggested_list items: { name, company, position, location, linkedin_url, score, signals, reasons, source_investor_id }.
    """
    investors = load_company_investors(company_id, db)
    if not investors:
        company_row = db.execute("SELECT name FROM companies WHERE id = ?", (company_id,)).fetchone()
        company_name = company_row["name"] if company_row else f"Company {company_id}"
        return [], company_name, 0, 0, {}

    company_row = db.execute("SELECT name FROM companies WHERE id = ?", (company_id,)).fetchone()
    company_name = company_row["name"] if company_row else f"Company {company_id}"

    investor_ids = [inv["id"] for inv in investors]
    connections = load_connections_for_investors(investor_ids, db)

    company_profile = build_company_profile(investors)
    company_connection_counts = _connection_company_counts(connections)

    scored = score_candidates(connections, company_profile, company_connection_counts)

    # Merge by candidate identity (same person from different source investors); track source_investor_ids
    by_key = {}
    for item in scored:
        c = item["candidate"]
        key = normalize_name(c["name"])
        if not key:
            continue
        sid = c.get("source_investor_id")
        if key not in by_key:
            by_key[key] = {
                "candidate": c,
                "score": item["score"],
                "signals": dict(item["signals"]),
                "reasons": list(item["reasons"]),
                "source_investor_ids": {sid} if sid else set(),
            }
        else:
            existing = by_key[key]
            if item["score"] > existing["score"]:
                existing["score"] = item["score"]
            existing["signals"].update(item["signals"])
            for r in item["reasons"]:
                if r not in existing["reasons"]:
                    existing["reasons"].append(r)
            if sid:
                existing["source_investor_ids"].add(sid)

    merged = [v for v in by_key.values()]

    gated = apply_multi_signal_gate(merged, min_signal_categories)
    deduped = apply_dedup(gated, investors, fuzzy_threshold)

    deduped.sort(key=lambda x: (-x["score"], x["candidate"]["name"].lower()))

    # Build output list: flat dicts for template; include shared_investors_count, shared_org for fit/overlap
    common_companies = {k for k, v in company_connection_counts.items() if v >= 2}
    suggested_list = []
    for item in deduped[:top_n]:
        c = item["candidate"]
        shared_investors_count = len(item.get("source_investor_ids") or set())
        cand_company = (c.get("company") or "").strip().lower()
        shared_org_count = 1 if cand_company and cand_company in common_companies else 0
        suggested_list.append({
            "name": c["name"],
            "company": c.get("company") or "",
            "position": c.get("position") or "",
            "location": c.get("location") or "",
            "linkedin_url": c.get("linkedin_url") or "",
            "score": round(item["score"], 1),
            "signals": item["signals"],
            "reasons": item["reasons"],
            "source_investor_id": c.get("source_investor_id"),
            "shared_investors_count": shared_investors_count,
            "shared_org_count": shared_org_count,
        })

    return suggested_list, company_name, len(investors), len(connections), company_profile
