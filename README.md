# KYI — Know Your Investor (Mock)

Flask app for managing investors and their LinkedIn connections, with a company-scoped **Suggested Investors** feed driven by multi-signal matching and de-duplication.

## Recommendation pipeline (Suggested Investors)

### Signal categories (independent boolean flags)

Each candidate is scored against these **five signal categories**. A recommendation is only shown if **at least 2 distinct categories** fire (multi-signal gating).

| Key | Description |
|-----|-------------|
| **s_industry** | Candidate title/company contains industry tokens from your investors (e.g. Private Equity, SaaS). |
| **s_location** | Candidate location overlaps with investor/company location tokens (city, state, region). |
| **s_firm_type** | Candidate company looks like an investor org (capital, partners, ventures, fund, etc.) or is similar to an existing investor firm name. |
| **s_title_pattern** | Candidate title matches investor-like roles (Partner, Principal, VP, MD, Director, etc.). |
| **s_company_in_network** | Candidate’s company appears across multiple connections (common in your network). |

### Multi-signal gating rule (hard requirement)

- **Rule:** `unique_signal_categories_fired >= 2`
- Categories are distinct; duplicates (e.g. two industry tokens) count as one category.
- Default is “≥ 2 of any category”; configurable in `kyi/recommendations.py` via `MIN_SIGNAL_CATEGORIES`.

### De-duplication (hard requirement)

Existing investors for the company are **never** suggested again, even with name variants:

1. **linkedin_url** — Exact match (if both candidate and investor have a URL).
2. **normalized_name** — Name normalized (lowercase, no punctuation, no middle initials, collapsed spaces) and compared exactly.
3. **Fuzzy name + (company, title)** — If company and title match an existing investor, a lightweight similarity (e.g. `SequenceMatcher` ratio) is used; above a threshold (default **0.88**) the candidate is treated as the same person and excluded.

Normalization and fuzzy logic live in `kyi/normalization.py` and `kyi/recommendations.py` (`apply_dedup`).

### Where to tune weights, thresholds, and top N

- **kyi/recommendations.py**
  - `MIN_SIGNAL_CATEGORIES` — Minimum number of distinct signal categories (default `2`).
  - `FUZZY_NAME_THRESHOLD` — Dedup name similarity threshold (default `0.88`).
  - `DEFAULT_TOP_N` — Max number of suggestions returned (default `100`).
  - Inside `score_candidates()`, per-signal score increments (e.g. industry +4, location +3) can be adjusted.
- **kyi/normalization.py**
  - `FIRM_TYPE_TOKENS` — Keywords that indicate investor-like firms.
  - `TITLE_PATTERNS` — Title substrings that indicate investor-like roles.

---

## Phase 2: Fit Score, Overlap Intelligence, Access Map

### Fit Score (Potential Investor Rating 0–100)

Each suggested investor has a **Fit Score** (0–100) computed from weighted dimensions:

1. **Similarity to existing investors** (up to 30 pts): industry overlap, title pattern match, firm-type / similar firm.
2. **Network-based strength** (up to 35 pts): how many of your investors’ connection lists contain this person; whether their company appears across the network.
3. **Location overlap** (up to 20 pts): same metro/region tokens as your investors.
4. **Recency** (up to 15 pts): placeholder; no interaction timestamps yet, so score is neutral.

The UI shows **Fit factors** (top reasons) and a **breakdown** (similarity / network / location / recency points). Tuning: `kyi/fit_score.py` — `MAX_SIMILARITY_PTS`, `MAX_NETWORK_PTS`, `MAX_LOCATION_PTS`, `MAX_RECENCY_PTS` and the internal sub-scores (e.g. industry/title/firm).

### Overlap metrics (per company)

- **Unique people / orgs** — Distinct connections and organizations in your investors’ networks.
- **Overlap people / orgs** — People or orgs that appear in **more than one** investor’s network.
- **Overlap %** — Share of unique nodes that are overlapping (higher = more shared access).
- **Collapse rate** — Share of unique people who appear in ≥2 investor networks (“second-degree” becoming “first-degree” opportunities).
- **Top overlapping people / orgs** — Top 20 by how many investor networks they appear in.

Tuning: `kyi/overlap.py` — `compute_overlap_intelligence`; no thresholds to change, but you can adjust “overlap” definition (e.g. ≥2 vs ≥3 networks).

### Access Map (Orbit)

- **Nodes**: Inner ring = company investors; outer = connections (people) + organizations.
- **Edges**: Direct (investor → person); second-degree (person → org). **Weight** = 1 + boost when the person appears in multiple investors’ networks (thicker lines in the UI).
- Stored in `network_nodes` / `network_edges` per company. Build: `kyi/access_map.py` — `build_access_map(company_id, db, store=True)`.

### Phase 2 tuning (weights, thresholds, Top N)

- **Fit score**: `kyi/fit_score.py` — dimension max points and sub-scores.
- **Suggested Investors**: sort (relevance / fit_score / overlap / location), filters (industry, location, firm_type, title pattern), **Top N** (default 25, max 200) — controlled in the Suggested Investors UI and in `kyi/routes.py` (top_n, filter/sort params).
- **Access Map**: `kyi/access_map.py` — edge weight formula; `build_access_map` stores graph for retrieval.

## Running the app

```bash
pip install -r requirements.txt
python app.py
```

Then open **http://127.0.0.1:5000**. Use **Suggested Investors** to pick a company and view its feed.

## Data and CSV ingestion

- **Companies** — One default company is created on first run; investors belong to a company (`company_id`).
- **Investors** — Add from the UI; optionally prefill from Suggested Investors “Add as Investor” (keeps company).
- **Connections** — Upload LinkedIn-style CSV per investor in the investor’s **Orbit** tab; ingestion and orbit view are unchanged and work as before.
