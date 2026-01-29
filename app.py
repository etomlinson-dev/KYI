import os
import sqlite3
import csv
import io
import json
from datetime import datetime
from flask import Flask, g, redirect, render_template_string, request, url_for, flash

from kyi.routes import register_company_routes

APP_TITLE = "KYI — Know Your Investor (Mock)"
DB_PATH = os.path.join(os.path.dirname(__file__), "kyi.db")

app = Flask(__name__)
app.secret_key = "dev-secret-change-me"


# -----------------------------
# DB helpers
# -----------------------------
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON;")
    return g.db


@app.teardown_appcontext
def close_db(_):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    db.executescript(
        """
        -- Companies: recommendations are scoped per company
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS investors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            full_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            location TEXT,
            industry TEXT,
            firm TEXT,
            title TEXT,
            linkedin_url TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        -- Connections uploaded for a given investor (Orbit data)
        CREATE TABLE IF NOT EXISTS connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            investor_id INTEGER NOT NULL,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            company TEXT,
            position TEXT,
            location TEXT,
            linkedin_url TEXT,
            connected_on TEXT,
            created_at TEXT,
            FOREIGN KEY (investor_id) REFERENCES investors(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_connections_investor ON connections(investor_id);

        -- Phase 2: investor_profiles (derived tokens + stats per investor)
        CREATE TABLE IF NOT EXISTS investor_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            investor_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            industry_tokens TEXT,
            location_tokens TEXT,
            firm_type_tokens TEXT,
            title_tokens TEXT,
            updated_at TEXT,
            UNIQUE(investor_id)
        );

        -- Phase 2: candidate_suggestions (per company feed item, persisted)
        CREATE TABLE IF NOT EXISTS candidate_suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            candidate_name TEXT NOT NULL,
            candidate_title TEXT,
            candidate_company TEXT,
            candidate_location TEXT,
            linkedin_url TEXT,
            fit_score INTEGER,
            relevance_score REAL,
            signals_fired TEXT,
            reasons TEXT,
            overlap_stats TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        -- Phase 2: network_nodes (access map graph)
        CREATE TABLE IF NOT EXISTS network_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            node_type TEXT NOT NULL,
            label TEXT,
            meta_json TEXT,
            created_at TEXT
        );

        -- Phase 2: network_edges (access map graph)
        CREATE TABLE IF NOT EXISTS network_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            from_node_id INTEGER NOT NULL,
            to_node_id INTEGER NOT NULL,
            edge_type TEXT NOT NULL,
            weight REAL DEFAULT 1,
            meta_json TEXT,
            created_at TEXT
        );

        -- Phase 3: interactions (relationship & behavior event log)
        CREATE TABLE IF NOT EXISTS interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            actor_type TEXT,              -- user | system
            entity_type TEXT NOT NULL,    -- investor | candidate | org
            entity_id INTEGER,            -- when pointing at investors/orgs
            entity_key TEXT,              -- stable key for candidates (e.g. normalized name + linkedin_url)
            event_type TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            meta_json TEXT
        );

        -- Phase 3: relationships (cached relationship strength)
        CREATE TABLE IF NOT EXISTS relationships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            from_type TEXT NOT NULL,
            from_id INTEGER,
            from_key TEXT,
            to_type TEXT NOT NULL,
            to_id INTEGER,
            to_key TEXT,
            relationship_strength INTEGER,
            strength_factors TEXT,
            last_interaction_ts TEXT,
            updated_at TEXT
        );

        -- Phase 3: investor behavior profiles
        CREATE TABLE IF NOT EXISTS investor_behavior_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            investor_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            axis_scores TEXT,
            confidence TEXT,
            behavior_metrics TEXT,
            updated_at TEXT,
            UNIQUE(investor_id, company_id)
        );

        -- Phase 3: tags and status history
        CREATE TABLE IF NOT EXISTS investor_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            investor_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            tag TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS relationship_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            relationship_id INTEGER NOT NULL,
            tag TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS investor_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            entity_type TEXT NOT NULL,   -- investor | candidate
            entity_id INTEGER,
            entity_key TEXT,
            status TEXT NOT NULL,
            ts TEXT NOT NULL,
            by_user TEXT
        );

        -- Phase 4: scenarios + runs
        CREATE TABLE IF NOT EXISTS scenarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            scenario_type TEXT NOT NULL,
            assumptions_json TEXT,
            created_by TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS scenario_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scenario_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            run_ts TEXT NOT NULL,
            results_json TEXT,
            confidence_score REAL,
            model_version TEXT
        );

        -- Phase 4: investor reaction model configs (rules-based)
        CREATE TABLE IF NOT EXISTS investor_reaction_models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL,
            weights_json TEXT,
            created_at TEXT
        );

        -- Phase 4: negotiation intelligence term sheets
        CREATE TABLE IF NOT EXISTS term_sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            investor_id INTEGER NOT NULL,
            round_name TEXT,
            received_ts TEXT,
            parsed_terms_json TEXT,
            source TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS clause_library (
            clause_key TEXT PRIMARY KEY,
            description TEXT,
            risk_category TEXT,
            default_weight REAL
        );

        CREATE TABLE IF NOT EXISTS investor_clause_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            investor_id INTEGER NOT NULL,
            clause_stats_json TEXT,
            founder_friendliness_score INTEGER,
            control_risk_score INTEGER,
            updated_at TEXT,
            UNIQUE(company_id, investor_id)
        );

        -- Phase 4: structured outcomes (safe, aggregate-only)
        CREATE TABLE IF NOT EXISTS outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            investor_id INTEGER NOT NULL,
            portfolio_company TEXT,
            outcome_type TEXT NOT NULL,
            outcome_ts TEXT NOT NULL,
            meta_json TEXT
        );

        -- Phase 4: Network Leverage Index snapshots
        CREATE TABLE IF NOT EXISTS network_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            snapshot_month TEXT NOT NULL,  -- YYYY-MM-01
            metrics_json TEXT,
            created_at TEXT,
            UNIQUE(company_id, snapshot_month)
        );
        """
    )
    db.commit()

    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_investor_profiles_company ON investor_profiles(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_candidate_suggestions_company ON candidate_suggestions(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_network_nodes_company ON network_nodes(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_network_edges_company ON network_edges(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_interactions_company ON interactions(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_interactions_entity ON interactions(entity_type, entity_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_interactions_entity_key ON interactions(entity_type, entity_key)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_relationships_company ON relationships(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_relationships_from_to ON relationships(from_type, from_id, to_type, to_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_investor_behavior_company ON investor_behavior_profiles(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_investor_tags_investor ON investor_tags(investor_id, company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_status_history_company ON investor_status_history(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_scenarios_company ON scenarios(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_scenario_runs_company ON scenario_runs(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_term_sheets_company_investor ON term_sheets(company_id, investor_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_investor_clause_patterns_company ON investor_clause_patterns(company_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_company_investor ON outcomes(company_id, investor_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_network_snapshots_company ON network_snapshots(company_id)")
        db.commit()
    except Exception:
        pass

    # Migration: add company_id to existing investors table if missing (must run before index on company_id)
    try:
        info = db.execute("PRAGMA table_info(investors)").fetchall()
        cols = [row[1] for row in info]
        if "company_id" not in cols:
            db.execute("ALTER TABLE investors ADD COLUMN company_id INTEGER")
            db.commit()
    except Exception:
        pass

    # Ensure default company exists and all investors have a company
    db.execute(
        "INSERT OR IGNORE INTO companies (id, name, created_at) VALUES (1, ?, ?)",
        ("Default Company", now_iso()),
    )
    db.execute("UPDATE investors SET company_id = 1 WHERE company_id IS NULL")
    db.commit()

    # Index on company_id (after column exists)
    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_investors_company ON investors(company_id)")
        db.commit()
    except Exception:
        pass


@app.before_request
def _ensure_db():
    init_db()


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")


# -----------------------------
# CSV parsing (LinkedIn-friendly)
# -----------------------------
def normalize_header(h: str) -> str:
    return (h or "").strip().lower().replace("\ufeff", "").replace(" ", "").replace("_", "").replace("-", "")


def get_first(row, keys, default=""):
    """
    Tries keys in order. Keys are already normalized.
    Also tries partial matches (contains) for more flexibility.
    """
    # First try exact matches
    for k in keys:
        if k in row and row[k] is not None and str(row[k]).strip() != "":
            return str(row[k]).strip()
    
    # Then try partial matches (header contains key)
    for k in keys:
        for header_key in row.keys():
            if k in header_key and row[header_key] is not None and str(row[header_key]).strip() != "":
                return str(row[header_key]).strip()
    
    return default


def parse_connections_csv(file_bytes: bytes):
    """
    Accepts LinkedIn connections export OR "close enough" CSVs.
    Expected fields (any subset):
      - First Name / Last Name OR Name
      - Company
      - Position
      - Location
      - Connected On
      - URL / Profile URL / LinkedIn URL
    """
    text = file_bytes.decode("utf-8", errors="ignore")
    f = io.StringIO(text)
    reader = csv.DictReader(f)

    if not reader.fieldnames:
        raise ValueError("CSV has no headers.")

    # Normalize headers
    normalized_fieldnames = [normalize_header(h) for h in reader.fieldnames]
    # Map original -> normalized by index
    # We'll rebuild each row dict with normalized keys.
    records = []
    row_count = 0
    for raw in reader:
        row_count += 1
        norm_row = {}
        # Store all values with multiple key variations for maximum flexibility
        for orig_key, norm_key in zip(reader.fieldnames, normalized_fieldnames):
            value = raw.get(orig_key, "")
            if value is None:
                value = ""
            # Store with normalized key
            norm_row[norm_key] = value
            # Also store with original key (lowercased, no spaces)
            if orig_key:
                orig_clean = orig_key.lower().strip().replace(" ", "").replace("_", "").replace("-", "")
                norm_row[orig_clean] = value
                # Also store exact original (lowercased)
                norm_row[orig_key.lower().strip()] = value

        first = get_first(norm_row, ["firstname", "first name", "first", "given name", "givenname"])
        last = get_first(norm_row, ["lastname", "last name", "last", "surname", "family name", "familyname"])
        name = get_first(norm_row, ["name", "full name", "fullname", "display name", "displayname"])

        if not name:
            name = (" ".join([first, last])).strip()

        company = get_first(norm_row, ["company", "organization", "org", "employer", "current company", "currentcompany"])
        position = get_first(norm_row, ["position", "title", "job title", "jobtitle", "headline", "job", "role"])
        location = get_first(norm_row, ["location", "geo", "region", "city", "country"])
        connected_on = get_first(norm_row, ["connected on", "connectedon", "connection date", "connectiondate", "date connected", "dateconnected", "connected"])
        linkedin_url = get_first(
            norm_row,
            ["url", "profile url", "profileurl", "linkedin url", "linkedinurl", "linkedin", "public profile url", "publicprofileurl", "profile"]
        )

        # Skip rows that are totally empty (must have at least a name)
        # Be more lenient - if we have any name component, include it
        if not name and not first and not last:
            continue

        # If first/last blank but name exists, try split
        if (not first and not last) and name:
            parts = name.split()
            if len(parts) >= 2:
                first = parts[0]
                last = " ".join(parts[1:])
            else:
                first = name
                last = ""

        records.append({
            "first_name": first,
            "last_name": last,
            "full_name": name,
            "company": company,
            "position": position,
            "location": location,
            "linkedin_url": linkedin_url,
            "connected_on": connected_on,
        })

    return records


# -----------------------------
# UI (single-template approach)
# -----------------------------
BASE_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{{ title }}</title>
  <style>
    :root{
      --bg:#0b0f14; --card:#121823; --muted:#9aa4b2; --text:#e8eef7; --line:#243043;
      --accent:#7c5cff; --accent2:#22c55e;
    }
    *{box-sizing:border-box}
    body{margin:0;background:linear-gradient(180deg,#070a0f 0%, #0b0f14 50%, #070a0f 100%); color:var(--text); font-family: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;}
    a{color:inherit;text-decoration:none}
    .wrap{max-width:1100px;margin:0 auto;padding:22px;}
    .topbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px}
    .brand{display:flex;gap:10px;align-items:center}
    .dot{width:10px;height:10px;border-radius:50%;background:var(--accent)}
    .h1{font-size:18px;font-weight:700;letter-spacing:.2px}
    .sub{font-size:13px;color:var(--muted)}
    .grid{display:grid;grid-template-columns: 1fr; gap:14px;}
    @media(min-width:900px){ .grid-2{grid-template-columns: 1.2fr .8fr; } }
    .card{background:rgba(18,24,35,.85); border:1px solid var(--line); border-radius:16px; padding:16px; box-shadow: 0 10px 30px rgba(0,0,0,.25);}
    .row{display:flex;gap:10px;flex-wrap:wrap}
    .btn{border:1px solid var(--line); background:#0d1320; color:var(--text); padding:10px 12px; border-radius:12px; cursor:pointer; font-weight:600}
    .btn:hover{border-color:#3a4a66}
    .btn.primary{background:linear-gradient(135deg,var(--accent),#3b82f6); border:0}
    .btn.good{background:linear-gradient(135deg,var(--accent2),#16a34a); border:0}
    input,select,textarea{width:100%; padding:10px 12px; border-radius:12px; border:1px solid var(--line); background:#0b1220; color:var(--text); outline:none}
    textarea{min-height:90px; resize:vertical}
    label{font-size:12px;color:var(--muted); display:block; margin-bottom:6px}
    .field{margin-bottom:10px}
    .table{width:100%; border-collapse:collapse; overflow:hidden; border-radius:12px; border:1px solid var(--line)}
    .table th,.table td{padding:10px 10px; border-bottom:1px solid var(--line); font-size:13px; vertical-align:top}
    .table th{color:var(--muted); text-align:left; font-weight:700; background:#0c1322}
    .pill{display:inline-flex; gap:6px; align-items:center; border:1px solid var(--line); padding:6px 10px; border-radius:999px; font-size:12px; color:var(--muted)}
    .flash{padding:10px 12px; border-radius:12px; border:1px solid #3a4a66; background:rgba(59,130,246,.12); margin-bottom:12px}
    .tabs{display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px}
    .tab{padding:8px 10px; border-radius:999px; border:1px solid var(--line); color:var(--muted); font-weight:700; font-size:12px}
    .tab.active{color:var(--text); border-color:#3a4a66; background:#0c1322}
    .muted{color:var(--muted)}
    .split{display:grid; grid-template-columns: 1fr; gap:12px}
    @media(min-width:900px){ .split{grid-template-columns: 1.15fr .85fr} }
    .kpi{display:grid; grid-template-columns: repeat(3, 1fr); gap:10px}
    .kpi .card{padding:12px}
    .kpi .n{font-size:20px; font-weight:900}
    .kpi .t{font-size:12px;color:var(--muted)}
    .small{font-size:12px}
    .hr{height:1px;background:var(--line);margin:12px 0}
    .right{display:flex; justify-content:flex-end; gap:8px}
  </style>

  <!-- vis-network for Orbit graph -->
  <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <div class="dot"></div>
        <div>
          <div class="h1">{{ title }}</div>
          <div class="sub">{{ subtitle }}</div>
        </div>
      </div>
      <div class="row">
        <a class="btn" href="{{ url_for('companies_list') }}">Companies</a>
        <a class="btn" href="{{ url_for('cross_reference') }}">Cross-Reference</a>
        <a class="btn primary" href="{{ url_for('new_investor') }}">+ Add Investor</a>
      </div>
    </div>

    {% with messages = get_flashed_messages() %}
      {% if messages %}
        {% for m in messages %}
          <div class="flash">{{ m }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}

    {{ body|safe }}
  </div>
</body>
</html>
"""


def render_page(body_html, subtitle="Know Your Investor mock"):
    return render_template_string(
        BASE_HTML,
        title=APP_TITLE,
        subtitle=subtitle,
        body=body_html
    )


# Register company-scoped routes (Suggested Investors per company)
register_company_routes(app, get_db, render_page)


# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def index():
    """Redirect to Companies list."""
    return redirect(url_for("companies_list"))


@app.route("/investors/new", methods=["GET", "POST"])
def new_investor():
    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip()
        if not full_name:
            flash("Name is required.")
            return redirect(url_for("new_investor"))

        db = get_db()
        now = now_iso()
        company_id = request.form.get("company_id") or request.args.get("company_id") or 1
        try:
            company_id = int(company_id)
        except (TypeError, ValueError):
            company_id = 1
        db.execute(
            """
            INSERT INTO investors
            (company_id, full_name, email, phone, location, industry, firm, title, linkedin_url, notes, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                company_id,
                full_name,
                (request.form.get("email") or "").strip(),
                (request.form.get("phone") or "").strip(),
                (request.form.get("location") or "").strip(),
                (request.form.get("industry") or "").strip(),
                (request.form.get("firm") or "").strip(),
                (request.form.get("title") or "").strip(),
                (request.form.get("linkedin_url") or "").strip(),
                (request.form.get("notes") or "").strip(),
                now,
                now,
            ),
        )
        db.commit()
        investor_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        flash("Investor created.")
        return redirect(url_for("investor_profile", investor_id=investor_id))

    body = """
    <div class="grid grid-2">
      <div class="card">
        <div style="font-size:16px;font-weight:900">Add Investor</div>
        <div class="muted small">Create Kevin Wallace, then click into the profile and upload connections on the Orbit tab.</div>
        <div class="hr"></div>
        <form method="POST">
          <div class="field">
            <label>Company *</label>
            <select name="company_id" required>
              {% for c in companies %}
              <option value="{{ c.id }}" {% if c.id == selected_company_id %}selected{% endif %}>{{ c.name }}</option>
              {% endfor %}
            </select>
            <div class="muted small" style="margin-top:4px">Investor will be associated with this company for Suggested Investors.</div>
          </div>
          <div class="field">
            <label>Full Name *</label>
            <input name="full_name" placeholder="Kevin Wallace" value="{{ prefill_name }}" required />
          </div>

          <div class="split">
            <div class="field">
              <label>Email</label>
              <input name="email" placeholder="kevin@dwgc.com" />
            </div>
            <div class="field">
              <label>Phone</label>
              <input name="phone" placeholder="(555) 555-5555" />
            </div>
          </div>

          <div class="split">
            <div class="field">
              <label>Location</label>
              <input name="location" placeholder="Philadelphia, PA" value="{{ prefill_location }}" />
            </div>
            <div class="field">
              <label>Industry</label>
              <input name="industry" placeholder="Private Equity / Ops / SaaS" />
            </div>
          </div>

          <div class="split">
            <div class="field">
              <label>Firm / Organization</label>
              <input name="firm" placeholder="DW Growth & Capital" value="{{ prefill_firm }}" />
            </div>
            <div class="field">
              <label>Title</label>
              <input name="title" placeholder="COO / Partner" />
            </div>
          </div>

          <div class="field">
            <label>LinkedIn URL</label>
            <input name="linkedin_url" placeholder="https://www.linkedin.com/in/..." />
          </div>

          <div class="field">
            <label>Notes</label>
            <textarea name="notes" placeholder="Anything important about this investor..."></textarea>
          </div>

          <div class="right">
            <a class="btn" href="/">Cancel</a>
            <button class="btn primary" type="submit">Create</button>
          </div>
        </form>
      </div>

      <div class="card">
        <div style="font-size:16px;font-weight:900">What this mock supports</div>
        <div class="hr"></div>
        <div class="pill">✓  Investor Profiles</div>
        <div class="pill">✓  Tabs (Info / Orbit)</div>
        <div class="pill">✓  Orbit from CSV Upload</div>
        <div class="pill">✓  Companies outer ring</div>
        <div class="pill">✓  People inner ring</div>
        <div class="pill">✓  Edges person → company</div>
        <div class="muted small" style="margin-top:10px">
          Upload a LinkedIn connections export CSV in the Orbit tab. It will dedupe companies and auto-generate the orbit graph.
        </div>
      </div>
    </div>
    """
    # Get URL parameters for pre-filling (e.g. from Suggested Investors "Add as Investor")
    prefill_name = request.args.get("name", "")
    prefill_firm = request.args.get("company", "")  # connection's company -> firm/organization field
    prefill_location = request.args.get("location", "")
    selected_company_id = request.args.get("company_id", "1")
    try:
        selected_company_id = int(selected_company_id)
    except (TypeError, ValueError):
        selected_company_id = 1

    db = get_db()
    companies = db.execute("SELECT id, name FROM companies ORDER BY name").fetchall()
    companies = [dict(r) for r in companies]
    if not companies:
        companies = [{"id": 1, "name": "Default Company"}]

    body = render_template_string(body, companies=companies, selected_company_id=selected_company_id,
        prefill_name=prefill_name, prefill_location=prefill_location, prefill_firm=prefill_firm)

    return render_page(body, subtitle="Create investor")


@app.route("/investor/<int:investor_id>")
def investor_profile(investor_id: int):
    tab = (request.args.get("tab") or "info").lower().strip()
    if tab not in ("info", "orbit", "relationship", "behavior"):
        tab = "info"

    db = get_db()
    inv = db.execute("SELECT * FROM investors WHERE id = ?", (investor_id,)).fetchone()
    if not inv:
        flash("Investor not found.")
        return redirect(url_for("index"))

    # Orbit counts
    conn_count = db.execute(
        "SELECT COUNT(*) AS c FROM connections WHERE investor_id = ?",
        (investor_id,)
    ).fetchone()["c"]

    company_count = db.execute(
        "SELECT COUNT(DISTINCT COALESCE(NULLIF(TRIM(company),''),'(No Company)')) AS c FROM connections WHERE investor_id = ?",
        (investor_id,)
    ).fetchone()["c"]
    
    # Debug: Get sample of stored connections to verify
    sample_conns = db.execute(
        "SELECT full_name, company, position FROM connections WHERE investor_id = ? LIMIT 5",
        (investor_id,)
    ).fetchall()

    # Tabs UI
    tabs_html = render_template_string(
        """
        <div class="tabs">
          <a class="tab {{ 'active' if tab=='info' else '' }}" href="{{ url_for('investor_profile', investor_id=inv['id'], tab='info') }}">Info</a>
          <a class="tab {{ 'active' if tab=='orbit' else '' }}" href="{{ url_for('investor_profile', investor_id=inv['id'], tab='orbit') }}">Orbit</a>
          <a class="tab {{ 'active' if tab=='relationship' else '' }}" href="{{ url_for('investor_profile', investor_id=inv['id'], tab='relationship') }}">Relationship</a>
          <a class="tab {{ 'active' if tab=='behavior' else '' }}" href="{{ url_for('investor_profile', investor_id=inv['id'], tab='behavior') }}">Behavior</a>
        </div>
        """,
        inv=inv, tab=tab
    )

    header_html = render_template_string(
        """
        <div class="card">
          <div class="row" style="justify-content:space-between;align-items:flex-start">
            <div>
              <div style="font-size:18px;font-weight:950">{{ inv['full_name'] }}</div>
              <div class="muted small">
                {{ inv['firm'] or '' }}{% if inv['firm'] and inv['title'] %} \'95 {% endif %}{{ inv['title'] or '' }}
              </div>
              <div class="row" style="margin-top:8px">
                {% if inv['email'] %}<div class="pill">📧  {{ inv['email'] }}</div>{% endif %}
                {% if inv['phone'] %}<div class="pill">📞  {{ inv['phone'] }}</div>{% endif %}
                {% if inv['location'] %}<div class="pill">📍  {{ inv['location'] }}</div>{% endif %}
                {% if inv['industry'] %}<div class="pill">🏢  {{ inv['industry'] }}</div>{% endif %}
              </div>
            </div>
            <div class="row">
              <a class="btn" href="{{ url_for('edit_investor', investor_id=inv['id']) }}">Edit</a>
              <a class="btn" href="{{ url_for('delete_investor', investor_id=inv['id']) }}" onclick="return confirm('Delete this investor and all orbit data?')">Delete</a>
            </div>
          </div>
        </div>
        """,
        inv=inv
    )

    # Tab bodies
    if tab == "info":
        body_inner = render_template_string(
            """
            <div class="card">
              <div style="font-size:16px;font-weight:900">Investor Information</div>
              <div class="hr"></div>

              <div class="split">
                <div>
                  <div class="field">
                    <label>Full Name</label>
                    <div>{{ inv['full_name'] }}</div>
                  </div>
                  <div class="field">
                    <label>Email</label>
                    <div class="muted">{{ inv['email'] or '—' }}</div>
                  </div>
                  <div class="field">
                    <label>Phone</label>
                    <div class="muted">{{ inv['phone'] or '—' }}</div>
                  </div>
                  <div class="field">
                    <label>Location</label>
                    <div class="muted">{{ inv['location'] or '—' }}</div>
                  </div>
                </div>

                <div>
                  <div class="field">
                    <label>Firm / Organization</label>
                    <div class="muted">{{ inv['firm'] or '—' }}</div>
                  </div>
                  <div class="field">
                    <label>Title</label>
                    <div class="muted">{{ inv['title'] or '—' }}</div>
                  </div>
                  <div class="field">
                    <label>Industry</label>
                    <div class="muted">{{ inv['industry'] or '—' }}</div>
                  </div>
                  <div class="field">
                    <label>LinkedIn URL</label>
                    {% if inv['linkedin_url'] %}
                      <div><a class="btn" href="{{ inv['linkedin_url'] }}" target="_blank">Open LinkedIn</a></div>
                    {% else %}
                      <div class="muted">—</div>
                    {% endif %}
                  </div>
                </div>
              </div>

              <div class="field">
                <label>Notes</label>
                <div class="muted" style="white-space:pre-wrap">{{ inv['notes'] or '—' }}</div>
              </div>
            </div>
            """,
            inv=inv
        )

    elif tab == "orbit":
        # Orbit tab: pull connections and build graph data
        rows = db.execute(
            """
            SELECT * FROM connections
            WHERE investor_id = ?
            ORDER BY created_at DESC
            LIMIT 5000
            """,
            (investor_id,)
        ).fetchall()

        # Build deduped sets for JS
        # Companies (outer ring)
        companies = {}
        people = []

        for r in rows:
            company = (r["company"] or "").strip()
            if not company:
                company = "(No Company)"
            companies.setdefault(company, 0)
            companies[company] += 1

            people.append({
                "id": r["id"],
                "name": (r["full_name"] or "").strip() or (f"{r['first_name'] or ''} {r['last_name'] or ''}".strip()),
                "company": company,
                "title": (r["position"] or "").strip(),
                "location": (r["location"] or "").strip(),
                "linkedin_url": (r["linkedin_url"] or "").strip(),
            })

        company_list = [{"company": k, "count": v} for k, v in companies.items()]
        # sort companies by count desc then name
        company_list.sort(key=lambda x: (-x["count"], x["company"].lower()))

        body_inner = render_template_string(
            """
            <div class="grid grid-2">
              <div class="card">
                <div class="row" style="justify-content:space-between;align-items:center">
                  <div>
                    <div style="font-size:16px;font-weight:900">Orbit</div>
                    <div class="muted small">Outer ring = companies. Inner ring = people. Lines connect people →  their company.</div>
                  </div>
                  <div class="row">
                    <a class="btn" href="{{ url_for('clear_orbit', investor_id=inv['id']) }}" onclick="return confirm('Clear all uploaded connections for this investor?')">Clear Orbit</a>
                  </div>
                </div>

                <div class="hr"></div>

                <form method="POST" action="{{ url_for('upload_orbit', investor_id=inv['id']) }}" enctype="multipart/form-data">
                  <div class="field">
                    <label>Upload LinkedIn Connections CSV</label>
                    <input type="file" name="file" accept=".csv" required />
                    <div class="muted small" style="margin-top:6px">
                      Tip: LinkedIn\'92s export usually includes headers like \'93First Name, Last Name, Company, Position, Connected On\'94.
                      This tool also accepts \'93close enough\'94 CSVs.
                    </div>
                  </div>
                  <div class="right">
                    <button class="btn good" type="submit">Upload & Build Orbit</button>
                  </div>
                </form>

                <div class="hr"></div>

                <div id="network" style="height:560px;border-radius:14px;border:1px solid var(--line);background:#070a0f"></div>
                <div id="orbit-selection-details" class="muted small" style="margin-top:8px;min-height:32px"></div>
                <div class="muted small" style="margin-top:6px">
                  Loaded: <b>{{ conn_count }}</b> connections — <b>{{ company_count }}</b> companies
                  {% if conn_count == 0 %}
                    <div style="color:#f87171;margin-top:4px">⚠️ No connections found. Try uploading your CSV again.</div>
                  {% elif conn_count < 20 %}
                    <div style="color:#fbbf24;margin-top:4px">⚠️ Only {{ conn_count }} connections loaded. If you uploaded more, check your CSV format.</div>
                  {% endif %}
                </div>
              </div>

              <div class="card">
                <div style="font-size:16px;font-weight:900">Orbit Breakdown</div>
                <div class="hr"></div>

                <div class="kpi">
                  <div class="card">
                    <div class="n">{{ company_count }}</div>
                    <div class="t">Companies</div>
                  </div>
                  <div class="card">
                    <div class="n">{{ conn_count }}</div>
                    <div class="t">People</div>
                  </div>
                  <div class="card">
                    <div class="n">{{ (company_count + conn_count) }}</div>
                    <div class="t">Nodes</div>
                  </div>
                </div>

                <div class="hr"></div>

                <div style="font-weight:900;margin-bottom:8px">Top Companies</div>
                {% if companies %}
                  <table class="table">
                    <thead><tr><th>Company</th><th>Count</th></tr></thead>
                    <tbody>
                      {% for c in companies[:15] %}
                        <tr>
                          <td class="small">{{ c.company }}</td>
                          <td class="small muted">{{ c.count }}</td>
                        </tr>
                      {% endfor %}
                    </tbody>
                  </table>
                {% else %}
                  <div class="muted small">Upload a CSV to populate the orbit.</div>
                {% endif %}

                <div class="hr"></div>
                <div class="muted small">
                  Click nodes in the graph to see tooltips. The orbit layout is deterministic (rings) so it stays readable during your creative session.
                </div>
              </div>
            </div>

            <script>
              // ----- Data from server -----
              const companyList = {{ company_list | tojson }};
              const peopleList = {{ people | tojson }};

              // Build nodes & edges:
              // Companies = outer ring
              // People = inner ring
              const nodes = [];
              const edges = [];

              // Center anchor (not displayed) helps keep orbit stable
              const CENTER_ID = "CENTER";
              nodes.push({ id: CENTER_ID, label: "", shape: "dot", size: 1, physics: false, fixed: true, x: 0, y: 0, color: { opacity: 0 } });

              // Orbit radii (tweak if you want)
              const OUTER_R = 320;
              const INNER_R = 175;

              // Place companies evenly around outer ring
              const nCompanies = Math.max(companyList.length, 1);
              const companyIndex = {};
              for (let i = 0; i < companyList.length; i++) {
                const c = companyList[i].company;
                companyIndex[c] = i;

                const angle = (2 * Math.PI * i) / nCompanies;
                const x = OUTER_R * Math.cos(angle);
                const y = OUTER_R * Math.sin(angle);

                nodes.push({
                  id: "CO_" + i,
                  label: c,
                  x, y,
                  fixed: true,
                  physics: false,
                  shape: "box",
                  margin: 10,
                  font: { size: 14, color: "#e8eef7" },
                  color: {
                    background: "#0c1322",
                    border: "#243043",
                    highlight: { background: "#1b2640", border: "#3a4a66" }
                  },
                  title: `${c} (${companyList[i].count} connections)`
                });
              }

              // Place people around inner ring
              const nPeople = Math.max(peopleList.length, 1);
              for (let i = 0; i < peopleList.length; i++) {
                const p = peopleList[i];
                const angle = (2 * Math.PI * i) / nPeople;
                const x = INNER_R * Math.cos(angle);
                const y = INNER_R * Math.sin(angle);

                const label = p.name || "(No Name)";
                const tooltip = [
                  `<b>${label}</b>`,
                  p.title ? `<div>${p.title}</div>` : "",
                  p.location ? `<div class="muted">${p.location}</div>` : "",
                  p.company ? `<div style="margin-top:6px">🏢  ${p.company}</div>` : "",
                  p.linkedin_url ? `<div style="margin-top:6px"><a href="${p.linkedin_url}" target="_blank">Open LinkedIn</a></div>` : ""
                ].join("");

                const nodeId = "PE_" + p.id;
                nodes.push({
                  id: nodeId,
                  label: label,
                  x, y,
                  fixed: true,
                  physics: false,
                  shape: "dot",
                  size: 10,
                  font: { size: 12, color: "#e8eef7" },
                  color: {
                    background: "#7c5cff",
                    border: "#3a4a66",
                    highlight: { background: "#22c55e", border: "#3a4a66" }
                  },
                  title: tooltip
                });

                // Edge person -> company
                const idx = companyIndex[p.company] ?? null;
                if (idx !== null) {
                  edges.push({
                    from: nodeId,
                    to: "CO_" + idx,
                    arrows: "",
                    color: { color: "#243043" },
                    width: 1
                  });
                }
              }

              // Render with vis-network
              const container = document.getElementById("network");
              const nodesDS = new vis.DataSet(nodes);
              const edgesDS = new vis.DataSet(edges);
              const data = { nodes: nodesDS, edges: edgesDS };

              const options = {
                interaction: { hover: true, tooltipDelay: 80 },
                physics: { enabled: false },
                layout: { improvedLayout: false },
                edges: { smooth: { type: "continuous" } }
              };

              const network = new vis.Network(container, data, options);

              // Click behavior: show connected people/companies under the graph
              const selectionEl = document.getElementById("orbit-selection-details");

              // Build helper maps
              const peopleById = {};
              peopleList.forEach(p => { peopleById["PE_" + p.id] = p; });
              const companyPeople = {};
              peopleList.forEach(p => {
                const comp = p.company || "(No Company)";
                if (!companyPeople[comp]) companyPeople[comp] = [];
                companyPeople[comp].push(p);
              });
              const companyByNodeId = {};
              companyList.forEach((c, i) => { companyByNodeId["CO_" + i] = c.company; });

              function renderSelectionDetails(nodeId) {
                if (!selectionEl) return;
                if (!nodeId) {
                  selectionEl.innerHTML = "";
                  return;
                }
                if (nodeId === CENTER_ID) {
                  selectionEl.innerHTML = "";
                  return;
                }
                if (nodeId.startsWith("CO_")) {
                  const compName = companyByNodeId[nodeId] || "(No Company)";
                  const peopleAt = companyPeople[compName] || [];
                  if (!peopleAt.length) {
                    selectionEl.innerHTML = `Company: <b>${compName}</b> &mdash; no people found in this orbit.`;
                    return;
                  }
                  const lines = peopleAt.slice(0, 12).map(p => {
                    const parts = [p.name || "(No Name)"];
                    if (p.title) parts.push(`&middot; ${p.title}`);
                    if (p.location) parts.push(`&middot; ${p.location}`);
                    return `<div style="margin-left:8px;margin-top:2px">• ${parts.join(" ")}</div>`;
                  }).join("");
                  const more = peopleAt.length > 12 ? `<div class="muted small" style="margin-left:8px;margin-top:4px">... and ${peopleAt.length - 12} more at this company</div>` : "";
                  selectionEl.innerHTML = `<div>Company: <b>${compName}</b></div>${lines}${more}`;
                } else if (nodeId.startsWith("PE_")) {
                  const p = peopleById[nodeId];
                  if (!p) {
                    selectionEl.innerHTML = "";
                    return;
                  }
                  const parts = [];
                  parts.push(`<div>Person: <b>${p.name || "(No Name)"}</b></div>`);
                  if (p.title) parts.push(`<div class="muted small">${p.title}</div>`);
                  if (p.company) parts.push(`<div class="muted small" style="margin-top:2px">Company: ${p.company}</div>`);
                  if (p.location) parts.push(`<div class="muted small" style="margin-top:2px">Location: ${p.location}</div>`);
                  selectionEl.innerHTML = parts.join("");
                } else {
                  selectionEl.innerHTML = "";
                }
              }

              network.on("selectNode", params => {
                const nodeId = (params.nodes && params.nodes[0]) || null;
                renderSelectionDetails(nodeId);
              });
              network.on("deselectNode", () => renderSelectionDetails(null));
            </script>
            """,
            inv=inv,
            conn_count=conn_count,
            company_count=company_count,
            companies=company_list,
            company_list=company_list,
            people=people
        )

    elif tab == "relationship":
        company_id = inv["company_id"]
        interactions = db.execute(
            """
            SELECT event_type, event_ts, meta_json
            FROM interactions
            WHERE company_id = ? AND entity_type = 'investor' AND entity_id = ?
            ORDER BY event_ts DESC
            """,
            (company_id, investor_id),
        ).fetchall()
        status_row = db.execute(
            """
            SELECT status, ts FROM investor_status_history
            WHERE company_id = ? AND entity_type = 'investor' AND entity_id = ?
            ORDER BY ts DESC LIMIT 1
            """,
            (company_id, investor_id),
        ).fetchone()
        tags = db.execute(
            "SELECT tag FROM investor_tags WHERE company_id = ? AND investor_id = ? ORDER BY tag",
            (company_id, investor_id),
        ).fetchall()
        body_inner = render_template_string(
            """
            <div class="card">
              <div style="font-size:16px;font-weight:900">Relationship</div>
              <div class="hr"></div>
              <div class="split">
                <div>
                  <div class="field">
                    <label>Current Status</label>
                    <form method="POST" action="{{ url_for('update_investor_status', investor_id=inv['id']) }}" style="display:flex;gap:8px;align-items:center">
                      <select name="status">
                        {% set current = status_row['status'] if status_row else '' %}
                        {% for s in ['prospect','contacted','meeting','interested','committed','invested','inactive'] %}
                        <option value="{{ s }}" {{ 'selected' if s == current else '' }}>{{ s }}</option>
                        {% endfor %}
                      </select>
                      <button class="btn primary" type="submit" style="padding:6px 10px;font-size:12px">Update</button>
                    </form>
                    {% if status_row %}
                      <div class="muted small" style="margin-top:4px">Last updated {{ status_row['ts'] }}</div>
                    {% endif %}
                  </div>
                  <div class="field">
                    <label>Tags</label>
                    <div class="row" style="gap:6px;flex-wrap:wrap;margin-bottom:6px">
                      {% for t in tags %}
                        <span class="pill">{{ t['tag'] }}</span>
                      {% endfor %}
                      {% if not tags %}
                        <span class="muted small">No tags yet.</span>
                      {% endif %}
                    </div>
                    <form method="POST" action="{{ url_for('add_investor_tag', investor_id=inv['id']) }}" style="display:flex;gap:6px">
                      <input name="tag" placeholder="e.g. priority, angel, follow-up" />
                      <button class="btn" type="submit" style="padding:6px 10px;font-size:12px">Add Tag</button>
                    </form>
                  </div>
                </div>
                <div>
                  <div class="field">
                    <label>Add Interaction</label>
                    <form method="POST" action="{{ url_for('add_investor_interaction', investor_id=inv['id']) }}">
                      <div class="split">
                        <div class="field">
                          <select name="event_type">
                            {% for et in ['intro_sent','email_sent','email_reply','meeting_scheduled','meeting_completed','followup_sent','doc_shared','term_sheet_received','term_sheet_signed','commitment_made','investment_closed','declined','ghosted'] %}
                            <option value="{{ et }}">{{ et }}</option>
                            {% endfor %}
                          </select>
                        </div>
                        <div class="field">
                          <input name="notes" placeholder="Notes (optional)" />
                        </div>
                      </div>
                      <div class="right">
                        <button class="btn primary" type="submit" style="padding:6px 10px;font-size:12px">Log Interaction</button>
                      </div>
                    </form>
                  </div>
                </div>
              </div>
            </div>

            <div class="card" style="margin-top:16px">
              <div style="font-size:16px;font-weight:900;margin-bottom:8px">Interaction Timeline</div>
              <div class="hr"></div>
              {% if interactions %}
                <div style="max-height:400px;overflow-y:auto">
                  {% for ev in interactions %}
                  <div class="small" style="padding:6px 0;border-bottom:1px solid var(--line)">
                    <div style="font-weight:600">{{ ev['event_type'] }}</div>
                    <div class="muted small">{{ ev['event_ts'] }}</div>
                    {% if ev['meta_json'] %}
                      <div class="muted small">{{ ev['meta_json'] }}</div>
                    {% endif %}
                  </div>
                  {% endfor %}
                </div>
              {% else %}
                <div class="muted small">No interactions logged yet.</div>
              {% endif %}
            </div>
            """,
            inv=inv,
            interactions=interactions,
            status_row=status_row,
            tags=tags,
        )

    elif tab == "behavior":
        from kyi.behavior_profiles import compute_behavior_profile

        company_id = inv["company_id"]
        profile = compute_behavior_profile(company_id, investor_id, db)
        axes = profile.get("axis_scores", {})
        conf = profile.get("confidence", {})
        metrics = profile.get("behavior_metrics", {})

        body_inner = render_template_string(
            """
            <div class="card">
              <div style="font-size:16px;font-weight:900">Behavior Metrics</div>
              <div class="hr"></div>
              <div class="split">
                <div>
                  <div class="field">
                    <label>Avg time to decision (days)</label>
                    <div class="muted">{{ metrics.avg_time_to_decision_days if metrics.avg_time_to_decision_days is not none else '—' }}</div>
                  </div>
                  <div class="field">
                    <label>Avg meetings to decision</label>
                    <div class="muted">{{ metrics.avg_meetings_to_decision if metrics.avg_meetings_to_decision is not none else '—' }}</div>
                  </div>
                </div>
                <div>
                  <div class="field">
                    <label>Response rate</label>
                    <div class="muted">
                      {% if metrics.response_rate is not none %}
                        {{ (metrics.response_rate * 100) | round(1) }}%
                      {% else %}—{% endif %}
                    </div>
                  </div>
                  <div class="field">
                    <label>Priority style</label>
                    <div class="muted small">{{ metrics.priority_style or 'unknown' }}</div>
                  </div>
                  <div class="field">
                    <label>Reliability</label>
                    <div class="muted small">{{ metrics.reliability or 'unknown' }}</div>
                  </div>
                </div>
              </div>
            </div>

            <div class="card" style="margin-top:16px">
              <div style="font-size:16px;font-weight:900;margin-bottom:8px">Behavioral Axes</div>
              <div class="hr"></div>
              <div class="split">
                <div>
                  {% for key,label in [('risk_appetite','Risk Appetite'),('control_orientation','Control Orientation'),('patience','Patience / Time Horizon')] %}
                    <div class="field">
                      <label>{{ label }}</label>
                      <div class="muted small">
                        {{ axes.get(key, 50) | round(1) }} / 100
                        <span style="margin-left:6px;font-size:11px">confidence: {{ (conf.get(key, 0)*100) | round(0) }}%</span>
                      </div>
                    </div>
                  {% endfor %}
                </div>
                <div>
                  {% for key,label in [('stress_behavior','Stress Behavior'),('relationship_style','Relationship Style'),('conviction_strength','Conviction Strength')] %}
                    <div class="field">
                      <label>{{ label }}</label>
                      <div class="muted small">
                        {{ axes.get(key, 50) | round(1) }} / 100
                        <span style="margin-left:6px;font-size:11px">confidence: {{ (conf.get(key, 0)*100) | round(0) }}%</span>
                      </div>
                    </div>
                  {% endfor %}
                </div>
              </div>
            </div>
            """,
            axes=axes,
            conf=conf,
            metrics=metrics,
        )

    page = header_html + tabs_html + body_inner
    return render_page(page, subtitle=f"Investor Profile — {inv['full_name']}")


@app.route("/investor/<int:investor_id>/status", methods=["POST"])
def update_investor_status(investor_id: int):
    db = get_db()
    inv = db.execute("SELECT * FROM investors WHERE id = ?", (investor_id,)).fetchone()
    if not inv:
        flash("Investor not found.")
        return redirect(url_for("index"))
    status = (request.form.get("status") or "").strip().lower()
    if not status:
        flash("Status is required.")
        return redirect(url_for("investor_profile", investor_id=investor_id, tab="relationship"))
    now = now_iso()
    db.execute(
        """
        INSERT INTO investor_status_history (company_id, entity_type, entity_id, entity_key, status, ts, by_user)
        VALUES (?, 'investor', ?, NULL, ?, ?, ?)
        """,
        (inv["company_id"], investor_id, status, now, "user"),
    )
    db.commit()
    flash("Status updated.")
    return redirect(url_for("investor_profile", investor_id=investor_id, tab="relationship"))


@app.route("/investor/<int:investor_id>/tags", methods=["POST"])
def add_investor_tag(investor_id: int):
    db = get_db()
    inv = db.execute("SELECT * FROM investors WHERE id = ?", (investor_id,)).fetchone()
    if not inv:
        flash("Investor not found.")
        return redirect(url_for("index"))
    tag = (request.form.get("tag") or "").strip()
    if not tag:
        return redirect(url_for("investor_profile", investor_id=investor_id, tab="relationship"))
    db.execute(
        "INSERT INTO investor_tags (investor_id, company_id, tag) VALUES (?, ?, ?)",
        (investor_id, inv["company_id"], tag),
    )
    db.commit()
    flash("Tag added.")
    return redirect(url_for("investor_profile", investor_id=investor_id, tab="relationship"))


@app.route("/investor/<int:investor_id>/interactions", methods=["POST"])
def add_investor_interaction(investor_id: int):
    db = get_db()
    inv = db.execute("SELECT * FROM investors WHERE id = ?", (investor_id,)).fetchone()
    if not inv:
        flash("Investor not found.")
        return redirect(url_for("index"))
    event_type = (request.form.get("event_type") or "").strip()
    notes = (request.form.get("notes") or "").strip()
    if not event_type:
        flash("Event type is required.")
        return redirect(url_for("investor_profile", investor_id=investor_id, tab="relationship"))
    now = now_iso()
    meta = {}
    if notes:
        meta["notes"] = notes
    db.execute(
        """
        INSERT INTO interactions (company_id, actor_type, entity_type, entity_id, entity_key, event_type, event_ts, meta_json)
        VALUES (?, 'user', 'investor', ?, NULL, ?, ?, ?)
        """,
        (inv["company_id"], investor_id, event_type, now, json.dumps(meta) if meta else None),
    )
    db.commit()
    flash("Interaction logged.")
    return redirect(url_for("investor_profile", investor_id=investor_id, tab="relationship"))


@app.route("/investor/<int:investor_id>/edit", methods=["GET", "POST"])
def edit_investor(investor_id: int):
    db = get_db()
    inv = db.execute("SELECT * FROM investors WHERE id = ?", (investor_id,)).fetchone()
    if not inv:
        flash("Investor not found.")
        return redirect(url_for("index"))

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip()
        if not full_name:
            flash("Name is required.")
            return redirect(url_for("edit_investor", investor_id=investor_id))

        company_id = request.form.get("company_id") or inv.get("company_id") or 1
        try:
            company_id = int(company_id)
        except (TypeError, ValueError):
            company_id = 1
        db.execute(
            """
            UPDATE investors
            SET company_id=?, full_name=?, email=?, phone=?, location=?, industry=?, firm=?, title=?, linkedin_url=?, notes=?, updated_at=?
            WHERE id=?
            """,
            (
                company_id,
                full_name,
                (request.form.get("email") or "").strip(),
                (request.form.get("phone") or "").strip(),
                (request.form.get("location") or "").strip(),
                (request.form.get("industry") or "").strip(),
                (request.form.get("firm") or "").strip(),
                (request.form.get("title") or "").strip(),
                (request.form.get("linkedin_url") or "").strip(),
                (request.form.get("notes") or "").strip(),
                now_iso(),
                investor_id,
            ),
        )
        db.commit()
        flash("Investor updated.")
        return redirect(url_for("investor_profile", investor_id=investor_id, tab="info"))

    companies = db.execute("SELECT id, name FROM companies ORDER BY name").fetchall()
    companies = [dict(r) for r in companies]
    if not companies:
        companies = [{"id": 1, "name": "Default Company"}]
    body = render_template_string(
        """
        <div class="card">
          <div style="font-size:16px;font-weight:900">Edit Investor</div>
          <div class="hr"></div>
          <form method="POST">
            <div class="field">
              <label>Company</label>
              <select name="company_id">
                {% for c in companies %}
                <option value="{{ c['id'] }}" {% if c['id'] == (inv['company_id'] or 1) %}selected{% endif %}>{{ c['name'] }}</option>
                {% endfor %}
              </select>
            </div>
            <div class="field">
              <label>Full Name *</label>
              <input name="full_name" value="{{ inv['full_name'] }}" required />
            </div>

            <div class="split">
              <div class="field">
                <label>Email</label>
                <input name="email" value="{{ inv['email'] or '' }}" />
              </div>
              <div class="field">
                <label>Phone</label>
                <input name="phone" value="{{ inv['phone'] or '' }}" />
              </div>
            </div>

            <div class="split">
              <div class="field">
                <label>Location</label>
                <input name="location" value="{{ inv['location'] or '' }}" />
              </div>
              <div class="field">
                <label>Industry</label>
                <input name="industry" value="{{ inv['industry'] or '' }}" />
              </div>
            </div>

            <div class="split">
              <div class="field">
                <label>Firm / Organization</label>
                <input name="firm" value="{{ inv['firm'] or '' }}" />
              </div>
              <div class="field">
                <label>Title</label>
                <input name="title" value="{{ inv['title'] or '' }}" />
              </div>
            </div>

            <div class="field">
              <label>LinkedIn URL</label>
              <input name="linkedin_url" value="{{ inv['linkedin_url'] or '' }}" />
            </div>

            <div class="field">
              <label>Notes</label>
              <textarea name="notes">{{ inv['notes'] or '' }}</textarea>
            </div>

            <div class="right">
              <a class="btn" href="{{ url_for('investor_profile', investor_id=inv['id'], tab='info') }}">Cancel</a>
              <button class="btn primary" type="submit">Save</button>
            </div>
          </form>
        </div>
        """,
        inv=inv,
        companies=companies
    )
    return render_page(body, subtitle="Edit investor")


@app.route("/investor/<int:investor_id>/delete")
def delete_investor(investor_id: int):
    db = get_db()
    db.execute("DELETE FROM investors WHERE id = ?", (investor_id,))
    db.commit()
    flash("Investor deleted.")
    return redirect(url_for("index"))


@app.route("/investor/<int:investor_id>/orbit/upload", methods=["POST"])
def upload_orbit(investor_id: int):
    db = get_db()
    inv = db.execute("SELECT id FROM investors WHERE id=?", (investor_id,)).fetchone()
    if not inv:
        flash("Investor not found.")
        return redirect(url_for("index"))

    file = request.files.get("file")
    if not file:
        flash("No file uploaded.")
        return redirect(url_for("investor_profile", investor_id=investor_id, tab="orbit"))

    file_bytes = file.read()
    try:
        records = parse_connections_csv(file_bytes)
    except Exception as e:
        flash(f"CSV parse failed: {e}")
        return redirect(url_for("investor_profile", investor_id=investor_id, tab="orbit"))

    if not records:
        flash("No usable rows found in CSV. Make sure your CSV has headers and at least one row with a name.")
        return redirect(url_for("investor_profile", investor_id=investor_id, tab="orbit"))

    # Clear existing connections for this investor first (to avoid duplicates on re-upload)
    db.execute("DELETE FROM connections WHERE investor_id=?", (investor_id,))

    created_at = now_iso()
    inserted = 0
    skipped = 0
    errors = []
    for idx, r in enumerate(records):
        try:
            # Ensure we have at least a name
            if not r.get("full_name") and not (r.get("first_name") or r.get("last_name")):
                skipped += 1
                continue
                
            db.execute(
                """
                INSERT INTO connections
                (investor_id, first_name, last_name, full_name, company, position, location, linkedin_url, connected_on, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    investor_id,
                    r.get("first_name", ""),
                    r.get("last_name", ""),
                    r.get("full_name", ""),
                    r.get("company", ""),
                    r.get("position", ""),
                    r.get("location", ""),
                    r.get("linkedin_url", ""),
                    r.get("connected_on", ""),
                    created_at,
                ),
            )
            inserted += 1
        except Exception as e:
            skipped += 1
            if len(errors) < 5:  # Only store first few errors
                errors.append(str(e))
            # Continue with other records even if one fails
            continue

    # bump updated_at
    db.execute("UPDATE investors SET updated_at=? WHERE id=?", (now_iso(), investor_id))
    db.commit()

    msg = f"Orbit uploaded: {inserted} connections added"
    if len(records) > inserted:
        msg += f" (parsed {len(records)} rows, skipped {skipped})"
    if errors:
        msg += f". Errors: {', '.join(errors[:3])}"
    flash(msg)
    return redirect(url_for("investor_profile", investor_id=investor_id, tab="orbit"))


@app.route("/investor/<int:investor_id>/orbit/clear")
def clear_orbit(investor_id: int):
    db = get_db()
    db.execute("DELETE FROM connections WHERE investor_id=?", (investor_id,))
    db.execute("UPDATE investors SET updated_at=? WHERE id=?", (now_iso(), investor_id))
    db.commit()
    flash("Orbit cleared.")
    return redirect(url_for("investor_profile", investor_id=investor_id, tab="orbit"))


@app.route("/cross-reference", methods=["GET", "POST"])
def cross_reference():
    db = get_db()
    
    # Get all investors with connections
    investors = db.execute(
        """
        SELECT i.*, COUNT(c.id) as conn_count
        FROM investors i
        LEFT JOIN connections c ON i.id = c.investor_id
        GROUP BY i.id
        HAVING conn_count > 0
        ORDER BY i.full_name
        """
    ).fetchall()
    
    if request.method == "POST":
        selected_ids = request.form.getlist("investor_ids")
        if len(selected_ids) < 2:
            flash("Please select at least 2 investors to compare.")
            return redirect(url_for("cross_reference"))
        
        selected_ids = [int(id) for id in selected_ids]
        
        # Get connections for each selected investor
        investor_data = {}
        all_people = {}  # full_name -> list of investor_ids who have this person
        all_companies = {}  # company -> set of investor_ids
        company_people = {}  # company -> list of people
        
        for inv_id in selected_ids:
            inv = db.execute("SELECT * FROM investors WHERE id = ?", (inv_id,)).fetchone()
            conns = db.execute(
                "SELECT * FROM connections WHERE investor_id = ?",
                (inv_id,)
            ).fetchall()
            
            investor_data[inv_id] = {
                "investor": inv,
                "connections": conns,
                "people": [],
                "companies": set()
            }
            
            for conn in conns:
                full_name = (conn["full_name"] or "").strip()
                if not full_name:
                    full_name = f"{conn['first_name'] or ''} {conn['last_name'] or ''}".strip()
                
                if full_name:
                    if full_name not in all_people:
                        all_people[full_name] = []
                    if inv_id not in all_people[full_name]:
                        all_people[full_name].append(inv_id)
                    investor_data[inv_id]["people"].append(full_name)
                
                company = (conn["company"] or "").strip()
                if company:
                    if company not in all_companies:
                        all_companies[company] = []
                    if inv_id not in all_companies[company]:
                        all_companies[company].append(inv_id)
                    if company not in investor_data[inv_id]["companies"]:
                        investor_data[inv_id]["companies"].add(company)
                    
                    if company not in company_people:
                        company_people[company] = []
                    if full_name:
                        company_people[company].append({
                            "name": full_name,
                            "investor_id": inv_id,
                            "position": (conn["position"] or "").strip(),
                            "location": (conn["location"] or "").strip(),
                            "linkedin_url": (conn["linkedin_url"] or "").strip()
                        })
        
        # Find overlaps
        overlapping_people = {name: inv_ids for name, inv_ids in all_people.items() if len(inv_ids) > 1}
        shared_companies = {comp: inv_ids for comp, inv_ids in all_companies.items() if len(inv_ids) > 1}
        
        # Convert sets to lists for template rendering
        for inv_id in investor_data:
            investor_data[inv_id]["companies"] = list(investor_data[inv_id]["companies"])
        
        # Build comparison stats
        stats = {
            "total_investors": len(selected_ids),
            "total_unique_people": len(all_people),
            "overlapping_people_count": len(overlapping_people),
            "total_unique_companies": len(all_companies),
            "shared_companies_count": len(shared_companies),
        }
        
        # Sort shared companies by overlap count
        shared_companies_sorted = sorted(
            shared_companies.items(),
            key=lambda x: (len(x[1]), x[0].lower()),
            reverse=True
        )
        
        # Convert overlapping_people to sorted list for template
        overlapping_people_list = sorted(
            overlapping_people.items(),
            key=lambda x: (len(x[1]), x[0].lower()),
            reverse=True
        )
        
        # SIMPLIFIED: Find potential investors from shared companies
        # Get companies from shared companies (where overlaps exist)
        search_companies = set(list(shared_companies.keys()) if shared_companies else [])
        
        # Also get companies from overlapping people
        for person_name, inv_ids in overlapping_people.items():
            for inv_id in inv_ids:
                for conn in investor_data[inv_id]["connections"]:
                    full_name = (conn["full_name"] or "").strip()
                    if not full_name:
                        full_name = f"{conn['first_name'] or ''} {conn['last_name'] or ''}".strip()
                    if full_name == person_name:
                        company = (conn["company"] or "").strip()
                        if company:
                            search_companies.add(company)
                        break
        
        # Get existing investor names to exclude
        existing_names = set()
        for inv in db.execute("SELECT full_name FROM investors").fetchall():
            if inv["full_name"]:
                existing_names.add(inv["full_name"].strip().lower())
        
        # Find potential investors: people at shared/overlap companies from OTHER investors
        potential_investors = []
        potential_by_company = {}
        
        if search_companies:
            # Find all connections at these companies from investors NOT in the comparison
            company_list_str = ",".join(["?"] * len(search_companies))
            selected_ids_str = ",".join(["?"] * len(selected_ids))
            
            similar_conns = db.execute(
                f"""
                SELECT DISTINCT
                    c.full_name,
                    c.first_name,
                    c.last_name,
                    c.company,
                    c.position,
                    c.location,
                    c.linkedin_url,
                    i.full_name as source_investor,
                    i.id as source_investor_id
                FROM connections c
                JOIN investors i ON c.investor_id = i.id
                WHERE c.company IN ({company_list_str})
                AND c.investor_id NOT IN ({selected_ids_str})
                AND LOWER(TRIM(COALESCE(c.full_name, ''))) NOT IN (
                    SELECT LOWER(TRIM(full_name)) FROM investors WHERE full_name IS NOT NULL AND full_name != ''
                )
                AND (c.full_name IS NOT NULL AND c.full_name != '')
                ORDER BY c.company, c.full_name
                LIMIT 300
                """,
                list(search_companies) + selected_ids
            ).fetchall()
            
            for conn in similar_conns:
                full_name = (conn["full_name"] or "").strip()
                if not full_name:
                    full_name = f"{conn['first_name'] or ''} {conn['last_name'] or ''}".strip()
                
                if not full_name or full_name.lower() in existing_names:
                    continue
                
            company = (conn["company"] or "").strip() if conn["company"] else ""
            position = (conn["position"] or "").strip() if conn["position"] else ""
            location = (conn["location"] or "").strip() if conn["location"] else ""
            
            potential_investors.append({
                    "name": full_name,
                    "company": company,
                    "position": position,
                    "location": location,
                    "linkedin_url": (conn["linkedin_url"] or "").strip(),
                    "source_investor": conn["source_investor"],
                    "source_investor_id": conn["source_investor_id"],
                })
            
            # Group by company
            for inv in potential_investors:
                company = inv["company"] or "(No Company)"
                if company not in potential_by_company:
                    potential_by_company[company] = []
                potential_by_company[company].append(inv)
            
            # Sort within each company
            for company in potential_by_company:
                potential_by_company[company].sort(key=lambda x: x["name"].lower())
        
        # Keep similar_connections for backward compatibility (simplified)
        similar_connections = []
        similar_by_company = {}
        overlap_companies = search_companies
        
        body = render_template_string(
            """
            <div class="card">
              <div class="row" style="justify-content:space-between;align-items:center">
                <div>
                  <div style="font-size:18px;font-weight:900">Cross-Reference Results</div>
                  <div class="muted small">Comparing connections across selected investors</div>
                </div>
                <a class="btn" href="{{ url_for('cross_reference') }}">← New Comparison</a>
              </div>
              <div class="hr"></div>
              
              <div class="kpi" style="margin-bottom:20px">
                <div class="card">
                  <div class="n">{{ stats.total_unique_people }}</div>
                  <div class="t">Total Unique People</div>
                </div>
                <div class="card">
                  <div class="n">{{ stats.overlapping_people_count }}</div>
                  <div class="t">Overlapping People</div>
                </div>
                <div class="card">
                  <div class="n">{{ stats.total_unique_companies }}</div>
                  <div class="t">Total Companies</div>
                </div>
                <div class="card">
                  <div class="n">{{ stats.shared_companies_count }}</div>
                  <div class="t">Shared Companies</div>
                </div>
              </div>
              
              <div class="grid grid-2" style="margin-top:20px">
                <div class="card">
                  <div style="font-size:16px;font-weight:900;margin-bottom:12px">Investors Being Compared</div>
                  <div class="hr"></div>
                  {% for inv_id in selected_ids %}
                    {% set inv_data = investor_data[inv_id] %}
                      <div style="margin-bottom:12px;padding:10px;background:#0c1322;border-radius:8px">
                      <div style="font-weight:700">{{ inv_data.investor.full_name }}</div>
                      <div class="muted small">{{ inv_data.investor.firm or '' }}</div>
                      <div class="muted small" style="margin-top:4px">
                        {{ inv_data.connections|length }} connections • {{ inv_data.companies|list|length }} companies
                      </div>
                    </div>
                  {% endfor %}
                </div>
                
                <div class="card">
                  <div style="font-size:16px;font-weight:900;margin-bottom:12px">Overlapping People</div>
                  <div class="hr"></div>
                  {% if overlapping_people %}
                    <div style="max-height:400px;overflow-y:auto">
                      {% for person_name, inv_ids in overlapping_people_list[:50] %}
                        <div style="padding:8px;margin-bottom:6px;background:#0c1322;border-radius:6px;font-size:13px">
                          <div style="font-weight:600">{{ person_name }}</div>
                          <div class="muted small" style="margin-top:4px">
                            Connected to: 
                            {% for inv_id in inv_ids %}
                              <span class="pill">{{ investor_data[inv_id].investor.full_name }}</span>
                            {% endfor %}
                          </div>
                        </div>
                      {% endfor %}
                      {% if overlapping_people|length > 50 %}
                        <div class="muted small">... and {{ overlapping_people|length - 50 }} more</div>
                      {% endif %}
                    </div>
                  {% else %}
                    <div class="muted small">No overlapping people found.</div>
                  {% endif %}
                </div>
              </div>
              
              <div class="hr" style="margin:20px 0"></div>
              
              <div class="card">
                <div style="font-size:16px;font-weight:900;margin-bottom:12px">Shared Companies</div>
                <div class="muted small" style="margin-bottom:12px">Companies where multiple investors have connections</div>
                <div class="hr"></div>
                {% if shared_companies_sorted %}
                  <div style="max-height:500px;overflow-y:auto">
                    {% for company, inv_ids in shared_companies_sorted[:30] %}
                      <div style="padding:12px;margin-bottom:8px;background:#0c1322;border-radius:8px">
                        <div style="font-weight:700;font-size:14px;margin-bottom:8px">{{ company }}</div>
                        <div class="muted small" style="margin-bottom:8px">
                          Shared by: 
                          {% for inv_id in inv_ids %}
                            <span class="pill">{{ investor_data[inv_id].investor.full_name }}</span>
                          {% endfor %}
                        </div>
                        <div style="font-size:12px;color:var(--muted)">
                          {% set company_conns = company_people.get(company, []) %}
                          {% if company_conns %}
                            <div style="margin-top:6px">
                              <strong>Connections at this company:</strong>
                              <div style="margin-top:4px">
                                {% for person in company_conns[:10] %}
                                  <div style="padding:4px 0;border-bottom:1px solid var(--line)">
                                    <span>{{ person.name }}</span>
                                    {% if person.position %}
                                      <span class="muted"> — {{ person.position }}</span>
                                    {% endif %}
                                    <span class="muted small"> ({{ investor_data[person.investor_id].investor.full_name }})</span>
                                  </div>
                                {% endfor %}
                                {% if company_conns|length > 10 %}
                                  <div class="muted small" style="margin-top:4px">... and {{ company_conns|length - 10 }} more</div>
                                {% endif %}
                              </div>
                            </div>
                          {% endif %}
                        </div>
                      </div>
                    {% endfor %}
                    {% if shared_companies_sorted|length > 30 %}
                      <div class="muted small">... and {{ shared_companies_sorted|length - 30 }} more shared companies</div>
                    {% endif %}
                  </div>
                {% else %}
                  <div class="muted small">No shared companies found.</div>
                {% endif %}
              </div>
              
              {% if overlap_companies %}
              <div class="hr" style="margin:20px 0"></div>
              
              <div class="card">
                <div style="font-size:16px;font-weight:900;margin-bottom:12px">Find Similar Connections</div>
                <div class="muted small" style="margin-bottom:12px">
                  People at the same companies as your overlapping connections, from other investors' networks.
                  Use this to discover new potential connections based on overlap patterns.
                </div>
                <div class="hr"></div>
                
                {% if similar_connections %}
                  <div style="margin-bottom:12px">
                    <div class="pill" style="background:rgba(124,92,255,.2);border-color:#7c5cff">
                      Found {{ similar_connections|length }} similar connections at {{ overlap_companies|length }} overlap companies
                    </div>
                  </div>
                  
                  <div style="max-height:600px;overflow-y:auto">
                    {% for company, conns in similar_by_company.items() %}
                      <div style="padding:12px;margin-bottom:12px;background:#0c1322;border-radius:8px;border:1px solid var(--line)">
                        <div style="font-weight:700;font-size:14px;margin-bottom:8px;color:#7c5cff">
                          🏢 {{ company }}
                          <span class="muted small">({{ conns|length }} similar connections)</span>
                        </div>
                        <div style="margin-top:8px">
                          {% for conn in conns[:20] %}
                            <div style="padding:8px;margin-bottom:6px;background:#070a0f;border-radius:6px;font-size:13px">
                              <div style="display:flex;justify-content:space-between;align-items:start">
                                <div style="flex:1">
                                  <div style="font-weight:600">{{ conn.name }}</div>
                                  {% if conn.position %}
                                    <div class="muted small">{{ conn.position }}</div>
                                  {% endif %}
                                  {% if conn.location %}
                                    <div class="muted small">📍 {{ conn.location }}</div>
                                  {% endif %}
                                </div>
                                <div style="text-align:right">
                                  <div class="pill" style="font-size:11px">{{ conn.investor_name }}</div>
                                  {% if conn.linkedin_url %}
                                    <div style="margin-top:4px">
                                      <a href="{{ conn.linkedin_url }}" target="_blank" class="btn" style="padding:4px 8px;font-size:11px">LinkedIn</a>
                                    </div>
                                  {% endif %}
                                </div>
                              </div>
                            </div>
                          {% endfor %}
                          {% if conns|length > 20 %}
                            <div class="muted small" style="margin-top:8px">... and {{ conns|length - 20 }} more at this company</div>
                          {% endif %}
                        </div>
                      </div>
                    {% endfor %}
                  </div>
                {% else %}
                  <div class="muted small">
                    No similar connections found at overlap companies from other investors.
                  </div>
                {% endif %}
              </div>
              {% endif %}
              
              {% if potential_investors %}
              <div class="hr" style="margin:20px 0"></div>
              
              <div class="card">
                <div style="font-size:16px;font-weight:900;margin-bottom:12px">🎯 Potential Investors</div>
                <div class="muted small" style="margin-bottom:12px">
                  People at the same companies as your overlapping connections, from other investors' networks. 
                  These connections aren't in your investor list yet but work at companies where overlaps exist.
                </div>
                <div class="hr"></div>
                
                <div style="margin-bottom:12px">
                  <div class="pill" style="background:rgba(34,197,94,.2);border-color:#22c55e">
                    Found {{ potential_investors|length }} potential investors at {{ potential_by_company|length }} companies
                  </div>
                </div>
                
                <div style="max-height:600px;overflow-y:auto">
                  {% for company, invs in potential_by_company.items() %}
                    <div style="padding:12px;margin-bottom:12px;background:#0c1322;border-radius:8px;border:1px solid var(--line)">
                      <div style="font-weight:700;font-size:14px;margin-bottom:8px;color:#22c55e">
                        🏢 {{ company }}
                        <span class="muted small">({{ invs|length }} people)</span>
                      </div>
                      <div style="margin-top:8px">
                        {% for inv in invs[:20] %}
                          <div style="padding:8px;margin-bottom:6px;background:#070a0f;border-radius:6px;font-size:13px">
                            <div style="display:flex;justify-content:space-between;align-items:start">
                              <div style="flex:1">
                                <div style="font-weight:600">{{ inv.name }}</div>
                                {% if inv.position %}
                                  <div class="muted small" style="margin-top:2px">{{ inv.position }}</div>
                                {% endif %}
                                {% if inv.location %}
                                  <div class="muted small" style="margin-top:2px">📍 {{ inv.location }}</div>
                                {% endif %}
                              </div>
                              <div style="text-align:right;min-width:120px">
                                <div class="muted small" style="margin-bottom:4px">{{ inv.source_investor }}</div>
                                <div style="display:flex;gap:4px;flex-direction:column">
                                  {% if inv.linkedin_url %}
                                    <a href="{{ inv.linkedin_url }}" target="_blank" class="btn" style="padding:4px 8px;font-size:11px">LinkedIn</a>
                                  {% endif %}
                                  <a href="{{ url_for('new_investor') }}?name={{ inv.name|urlencode }}&company={{ inv.company|urlencode }}&location={{ inv.location|urlencode }}" 
                                     class="btn good" style="padding:4px 8px;font-size:11px">+ Add Investor</a>
                                </div>
                              </div>
                            </div>
                          </div>
                        {% endfor %}
                        {% if invs|length > 20 %}
                          <div class="muted small" style="margin-top:8px">... and {{ invs|length - 20 }} more at this company</div>
                        {% endif %}
                      </div>
                    </div>
                  {% endfor %}
                </div>
              </div>
              {% endif %}
            </div>
            """,
            selected_ids=selected_ids,
            investor_data=investor_data,
            overlapping_people=overlapping_people,
            overlapping_people_list=overlapping_people_list,
            shared_companies_sorted=shared_companies_sorted,
            company_people=company_people,
            stats=stats,
            overlap_companies=list(overlap_companies) if overlap_companies else [],
            similar_connections=similar_connections,
            similar_by_company=similar_by_company,
            potential_investors=potential_investors,
            potential_by_company=potential_by_company
        )
        return render_page(body, subtitle="Cross-Reference Analysis")
    
    # GET request - show selection form
    body = render_template_string(
        """
        <div class="card">
          <div style="font-size:18px;font-weight:900">Cross-Reference Investors</div>
          <div class="muted small" style="margin-bottom:20px">
            Select 2 or more investors to compare their connections, find overlaps, and identify shared companies.
          </div>
          <div class="hr"></div>
          
          {% if not investors %}
            <div class="muted">No investors with connections found. Upload connection CSVs first.</div>
          {% else %}
            <form method="POST">
              <div style="margin-bottom:16px">
                <label style="display:block;margin-bottom:8px;font-weight:600">Select Investors to Compare:</label>
                <div style="display:grid;grid-template-columns:repeat(auto-fill, minmax(250px, 1fr));gap:10px">
                  {% for inv in investors %}
                    <label style="display:flex;align-items:center;gap:8px;padding:10px;background:#0c1322;border-radius:8px;cursor:pointer;border:2px solid var(--line);transition:all 0.2s">
                      <input type="checkbox" name="investor_ids" value="{{ inv.id }}" style="width:18px;height:18px;cursor:pointer">
                      <div style="flex:1">
                        <div style="font-weight:600">{{ inv.full_name }}</div>
                        <div class="muted small">{{ inv.firm or '' }}</div>
                        <div class="muted small">{{ inv.conn_count }} connections</div>
                      </div>
                    </label>
                  {% endfor %}
                </div>
              </div>
              <div class="right">
                <button class="btn primary" type="submit">Compare Investors</button>
              </div>
            </form>
          {% endif %}
        </div>
        """,
        investors=investors
    )
    return render_page(body, subtitle="Cross-Reference")


@app.route("/potential-investors", methods=["GET", "POST"])
def potential_investors():
    db = get_db()
    
    # Get all existing investors to analyze patterns
    existing_investors = db.execute(
        "SELECT firm, industry, location FROM investors WHERE firm IS NOT NULL AND firm != ''"
    ).fetchall()
    
    # Extract patterns
    firms = set()
    industries = set()
    locations = set()
    firm_keywords = set()  # Extract keywords from firm names
    
    for inv in existing_investors:
        if inv["firm"]:
            firms.add(inv["firm"].strip())
            # Extract keywords (Capital, Growth, Partners, Ventures, etc.)
            firm_lower = inv["firm"].lower()
            keywords = ["capital", "growth", "partners", "ventures", "equity", "fund", "group", "holdings", "investments"]
            for kw in keywords:
                if kw in firm_lower:
                    firm_keywords.add(kw)
        
        if inv["industry"]:
            industries.add(inv["industry"].strip())
        
        if inv["location"]:
            locations.add(inv["location"].strip())
    
    # Get all existing investor names to exclude them
    existing_names = set()
    for inv in db.execute("SELECT full_name FROM investors").fetchall():
        if inv["full_name"]:
            existing_names.add(inv["full_name"].strip().lower())
    
    # Search connections for potential matches
    potential_matches = []
    
    if request.method == "POST":
        # Get filter criteria
        filter_firm = request.form.get("filter_firm", "").strip()
        filter_industry = request.form.get("filter_industry", "").strip()
        filter_location = request.form.get("filter_location", "").strip()
        filter_company = request.form.get("filter_company", "").strip()
        min_matches = int(request.form.get("min_matches", "1") or "1")
        
        # Build query
        conditions = []
        params = []
        
        # Exclude existing investors
        conditions.append("""
            LOWER(TRIM(c.full_name)) NOT IN (
                SELECT LOWER(TRIM(full_name)) FROM investors WHERE full_name IS NOT NULL
            )
        """)
        
        # Filter by company (firm similarity)
        if filter_company:
            conditions.append("(c.company LIKE ? OR c.company LIKE ?)")
            params.extend([f"%{filter_company}%", f"%{filter_company.lower()}%"])
        
        # Filter by location
        if filter_location:
            conditions.append("(c.location LIKE ? OR c.location LIKE ?)")
            params.extend([f"%{filter_location}%", f"%{filter_location.lower()}%"])
        
        # Look for firm-type keywords in company/position
        if filter_firm or firm_keywords:
            firm_conditions = []
            if filter_firm:
                firm_conditions.append("(c.company LIKE ? OR c.position LIKE ?)")
                params.extend([f"%{filter_firm}%", f"%{filter_firm}%"])
            for kw in firm_keywords:
                firm_conditions.append("(c.company LIKE ? OR c.position LIKE ?)")
                params.extend([f"%{kw}%", f"%{kw}%"])
            if firm_conditions:
                conditions.append(f"({' OR '.join(firm_conditions)})")
        
        # Look for industry keywords in position/company
        if filter_industry:
            conditions.append("(c.position LIKE ? OR c.company LIKE ?)")
            params.extend([f"%{filter_industry}%", f"%{filter_industry}%"])
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Get potential matches
        query = f"""
            SELECT DISTINCT
                c.full_name,
                c.first_name,
                c.last_name,
                c.company,
                c.position,
                c.location,
                c.linkedin_url,
                i.full_name as source_investor,
                i.id as source_investor_id,
                COUNT(DISTINCT c.id) as connection_count
            FROM connections c
            JOIN investors i ON c.investor_id = i.id
            WHERE {where_clause}
            GROUP BY c.full_name, c.company, c.position, c.location
            HAVING connection_count >= ?
            ORDER BY connection_count DESC, c.full_name
            LIMIT 500
        """
        params.append(min_matches)
        
        results = db.execute(query, params).fetchall()
        
        for r in results:
            full_name = (r["full_name"] or "").strip()
            if not full_name:
                full_name = f"{r['first_name'] or ''} {r['last_name'] or ''}".strip()
            
            if full_name.lower() in existing_names:
                continue
            
            # Calculate match score
            match_reasons = []
            score = 0
            
            company = (r["company"] or "").strip()
            position = (r["position"] or "").strip()
            location = (r["location"] or "").strip()
            
            # Check firm similarity
            if company:
                company_lower = company.lower()
                for firm in firms:
                    if firm.lower() in company_lower or company_lower in firm.lower():
                        match_reasons.append(f"Similar firm: {firm}")
                        score += 3
                        break
                for kw in firm_keywords:
                    if kw in company_lower:
                        match_reasons.append(f"Firm-type keyword: {kw}")
                        score += 2
                        break
            
            # Check industry similarity
            if position:
                position_lower = position.lower()
                for ind in industries:
                    if ind.lower() in position_lower:
                        match_reasons.append(f"Industry match: {ind}")
                        score += 2
                        break
            
            # Check location (only if connection has a location)
            if location and location.strip():
                location_lower = location.lower().strip()
                for loc in locations:
                    if loc and loc.strip():
                        if loc in location_lower or location_lower in loc:
                            match_reasons.append(f"Location match: {loc}")
                            score += 1
                            break
            
            potential_matches.append({
                "name": full_name,
                "company": company,
                "position": position,
                "location": location,
                "linkedin_url": (r["linkedin_url"] or "").strip(),
                "source_investor": r["source_investor"],
                "source_investor_id": r["source_investor_id"],
                "connection_count": r["connection_count"],
                "match_score": score,
                "match_reasons": match_reasons
            })
        
        # Sort by match score
        potential_matches.sort(key=lambda x: (-x["match_score"], -x["connection_count"], x["name"].lower()))
    
    # Get unique values for filters
    all_companies = db.execute(
        "SELECT DISTINCT company FROM connections WHERE company IS NOT NULL AND company != '' ORDER BY company LIMIT 200"
    ).fetchall()
    
    all_locations = db.execute(
        "SELECT DISTINCT location FROM connections WHERE location IS NOT NULL AND location != '' ORDER BY location LIMIT 200"
    ).fetchall()
    
    body = render_template_string(
        """
        <div class="card">
          <div style="font-size:18px;font-weight:900">Find Potential Investors</div>
          <div class="muted small" style="margin-bottom:20px">
            Search your connections for people who match patterns from existing investors (similar firms, industries, locations).
            These are connections that could be potential investors but aren't in your system yet.
          </div>
          <div class="hr"></div>
          
          <form method="POST">
            <div class="grid grid-2" style="margin-bottom:16px">
              <div class="field">
                <label>Company/Firm (contains)</label>
                <input name="filter_company" placeholder="e.g., Capital, Partners, Ventures" value="{{ request.form.get('filter_company', '') }}" />
                <div class="muted small" style="margin-top:4px">Search for firm-type keywords</div>
              </div>
              
              <div class="field">
                <label>Industry (in position/company)</label>
                <input name="filter_industry" placeholder="e.g., Private Equity, SaaS, Ops" value="{{ request.form.get('filter_industry', '') }}" />
              </div>
              
              <div class="field">
                <label>Location</label>
                <input name="filter_location" placeholder="e.g., Philadelphia, PA" value="{{ request.form.get('filter_location', '') }}" />
              </div>
              
              <div class="field">
                <label>Minimum Connections</label>
                <input type="number" name="min_matches" value="{{ request.form.get('min_matches', '1') }}" min="1" />
                <div class="muted small" style="margin-top:4px">Show people with at least this many connections</div>
              </div>
            </div>
            
            <div class="right">
              <button class="btn primary" type="submit">Search Potential Investors</button>
            </div>
          </form>
          
          {% if existing_investors %}
          <div class="hr" style="margin:20px 0"></div>
          <div style="margin-bottom:16px">
            <div style="font-weight:700;margin-bottom:8px">Current Investor Patterns:</div>
            <div class="row" style="flex-wrap:wrap;gap:6px">
              {% for firm in firms %}
                <span class="pill">{{ firm }}</span>
              {% endfor %}
              {% for ind in industries %}
                <span class="pill">{{ ind }}</span>
              {% endfor %}
              {% for loc in locations %}
                <span class="pill">{{ loc }}</span>
              {% endfor %}
            </div>
          </div>
          {% endif %}
        </div>
        
        {% if potential_matches %}
        <div class="card" style="margin-top:20px">
          <div style="font-size:16px;font-weight:900;margin-bottom:12px">
            Potential Investors Found: {{ potential_matches|length }}
          </div>
          <div class="hr"></div>
          
          <div style="max-height:700px;overflow-y:auto">
            {% for match in potential_matches %}
              <div style="padding:16px;margin-bottom:12px;background:#0c1322;border-radius:8px;border:1px solid var(--line)">
                <div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:8px">
                  <div style="flex:1">
                    <div style="font-size:16px;font-weight:700;margin-bottom:4px">{{ match.name }}</div>
                    {% if match.position %}
                      <div style="color:var(--muted);margin-bottom:4px">{{ match.position }}</div>
                    {% endif %}
                    {% if match.company %}
                      <div style="color:var(--muted);margin-bottom:4px">🏢 {{ match.company }}</div>
                    {% endif %}
                    {% if match.location %}
                      <div style="color:var(--muted);font-size:12px">📍 {{ match.location }}</div>
                    {% endif %}
                  </div>
                  <div style="text-align:right">
                    <div class="pill" style="background:rgba(124,92,255,.2);border-color:#7c5cff;margin-bottom:6px">
                      Score: {{ match.match_score }}
                    </div>
                    <div class="muted small">{{ match.connection_count }} connection(s)</div>
                  </div>
                </div>
                
                {% if match.match_reasons %}
                  <div style="margin-top:8px;padding:8px;background:#070a0f;border-radius:6px">
                    <div style="font-size:12px;font-weight:600;margin-bottom:4px;color:#7c5cff">Match Reasons:</div>
                    {% for reason in match.match_reasons %}
                      <div class="muted small" style="margin-left:8px">• {{ reason }}</div>
                    {% endfor %}
                  </div>
                {% endif %}
                
                <div style="margin-top:8px;display:flex;gap:8px;align-items:center">
                  <div class="muted small">From: <span class="pill">{{ match.source_investor }}</span></div>
                  {% if match.linkedin_url %}
                    <a href="{{ match.linkedin_url }}" target="_blank" class="btn" style="padding:4px 10px;font-size:12px">LinkedIn</a>
                  {% endif %}
                  <a href="{{ url_for('new_investor') }}?name={{ match.name|urlencode }}&company={{ match.company|urlencode }}&location={{ match.location|urlencode }}" 
                     class="btn good" style="padding:4px 10px;font-size:12px">+ Add as Investor</a>
                </div>
              </div>
            {% endfor %}
          </div>
        </div>
        {% elif request.method == 'POST' %}
        <div class="card" style="margin-top:20px">
          <div class="muted">No potential investors found matching your criteria. Try adjusting your filters.</div>
        </div>
        {% endif %}
        """,
        existing_investors=existing_investors,
        firms=list(firms),
        industries=list(industries),
        locations=list(locations),
        potential_matches=potential_matches,
        firm_keywords=list(firm_keywords)
    )
    return render_page(body, subtitle="Potential Investors")


@app.route("/find-similar", methods=["GET", "POST"])
def find_similar_investors():
    """Redirect to company-scoped Suggested Investors (legacy link)."""
    return redirect(url_for("companies_list"))




if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)