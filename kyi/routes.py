"""
Company-scoped route: Suggested Investors feed per company.
"""
from flask import redirect, render_template_string, request, url_for, flash, jsonify

from .recommendations import run_pipeline
from .fit_score import compute_fit_score
from .overlap import compute_overlap_intelligence, compute_investor_overlap_matrix
from .access_map import build_access_map, load_access_map, get_node_connections, get_all_investors_for_solar
from .relationship_strength import compute_investor_candidate_strength
from .forecasting_engine import run_scenario
from .negotiation_intelligence import update_investor_clause_patterns, get_investor_clause_profile, compare_investors
from .nli_metrics import compute_nli, get_nli_history

# Signal category labels for badges (human-readable).
SIGNAL_LABELS = {
    "s_industry": "Industry",
    "s_location": "Location",
    "s_firm_type": "Firm type",
    "s_title_pattern": "Title",
    "s_company_in_network": "Company in network",
}


def register_company_routes(app, get_db, render_page):
    """Register /companies, /companies/new, and /companies/<id>/suggested-investors on the Flask app."""

    def now_iso():
        from datetime import datetime
        return datetime.utcnow().isoformat(timespec="seconds")

    @app.route("/companies/new", methods=["GET", "POST"])
    def new_company():
        if request.method == "POST":
            name = (request.form.get("name") or "").strip()
            if not name:
                flash("Company name is required.")
                return redirect(url_for("new_company"))
            db = get_db()
            db.execute(
                "INSERT INTO companies (name, created_at) VALUES (?, ?)",
                (name, now_iso()),
            )
            db.commit()
            flash("Company created.")
            return redirect(url_for("companies_list"))
        body = render_template_string(
            """
            <div class="card">
              <div style="font-size:18px;font-weight:900">Add Company</div>
              <div class="muted small" style="margin-bottom:16px">
                Create a company to associate investors with. Suggested Investors are scoped per company.
              </div>
              <div class="hr"></div>
              <form method="POST">
                <div class="field">
                  <label>Company Name *</label>
                  <input name="name" placeholder="e.g. Acme Ventures" required />
                </div>
                <div class="right" style="margin-top:16px">
                  <a class="btn" href="{{ url_for('companies_list') }}">Cancel</a>
                  <button class="btn primary" type="submit">Create Company</button>
                </div>
              </form>
            </div>
            """
        )
        return render_page(body, subtitle="Add Company")

    @app.route("/companies")
    def companies_list():
        db = get_db()
        companies = db.execute(
            "SELECT c.id, c.name, c.created_at, "
            " (SELECT COUNT(*) FROM investors i WHERE i.company_id = c.id) AS investor_count"
            " FROM companies c ORDER BY c.name"
        ).fetchall()
        body = render_template_string(
            """
            <div class="card">
              <div class="row" style="justify-content:space-between;align-items:center;margin-bottom:16px">
                <div>
                  <div style="font-size:18px;font-weight:900">Companies</div>
                  <div class="muted small">Click into a company to view its investors, assign investors, or see suggested investors.</div>
                </div>
                <a class="btn primary" href="{{ url_for('new_company') }}">+ Add Company</a>
              </div>
              <div class="hr"></div>
              {% if companies %}
              <table class="table">
                <thead>
                  <tr>
                    <th>Company</th>
                    <th>Investors</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {% for c in companies %}
                  <tr>
                    <td style="font-weight:700">{{ c['name'] }}</td>
                    <td>{{ c['investor_count'] }}</td>
                    <td>
                      <div class="row" style="gap:6px;justify-content:flex-end;flex-wrap:wrap">
                        <a class="btn" href="{{ url_for('company_investors', company_id=c['id']) }}">View Investors</a>
                        <a class="btn" href="{{ url_for('assign_investors', company_id=c['id']) }}">Assign Investors</a>
                        <a class="btn" href="{{ url_for('access_map', company_id=c['id']) }}">Access Map</a>
                        <a class="btn primary" href="{{ url_for('suggested_investors', company_id=c['id']) }}">
                          Suggested Investors
                        </a>
                      </div>
                    </td>
                  </tr>
                  {% endfor %}
                </tbody>
              </table>
              {% else %}
              <div class="muted">No companies yet. Create a company from the Investors page (new investors default to Default Company).</div>
              {% endif %}
            </div>
            """,
            companies=[dict(r) for r in companies],
        )
        return render_page(body, subtitle="Companies")

    @app.route("/companies/<int:company_id>/investors")
    def company_investors(company_id):
        """View investors assigned to a company."""
        db = get_db()
        company = db.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        if not company:
            flash("Company not found.")
            return redirect(url_for("companies_list"))

        investors = db.execute(
            "SELECT * FROM investors WHERE company_id = ? ORDER BY updated_at DESC",
            (company_id,),
        ).fetchall()
        investors = [dict(r) for r in investors]

        body = render_template_string(
            """
            <div class="card">
              <div class="row" style="justify-content:space-between;align-items:center;margin-bottom:16px">
                <div>
                  <div style="font-size:18px;font-weight:900">Investors — {{ company['name'] }}</div>
                  <div class="muted small">Manage investors assigned to this company. Click into any investor to view their profile and connections.</div>
                </div>
                <a class="btn primary" href="{{ url_for('new_investor', company_id=company['id']) }}">+ Add Investor</a>
              </div>
              <div class="hr"></div>
              {% if investors %}
              <table class="table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Company / Firm</th>
                    <th>Location</th>
                    <th>Industry</th>
                    <th>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {% for inv in investors %}
                  <tr>
                    <td>
                      <a href="{{ url_for('investor_profile', investor_id=inv['id']) }}" style="font-weight:800">
                        {{ inv['full_name'] }}
                      </a>
                      <div class="muted small">{{ inv['email'] or '' }}</div>
                    </td>
                    <td class="small">
                      {{ inv['firm'] or '' }}
                      <div class="muted">{{ inv['title'] or '' }}</div>
                    </td>
                    <td class="small">{{ inv['location'] or '' }}</td>
                    <td class="small">{{ inv['industry'] or '' }}</td>
                    <td class="small muted">{{ (inv['updated_at'] or '')[:19].replace('T',' ') }}</td>
                  </tr>
                  {% endfor %}
                </tbody>
              </table>
              {% else %}
              <div class="muted">No investors assigned to this company yet. <a href="{{ url_for('assign_investors', company_id=company['id']) }}">Assign investors</a> or <a href="{{ url_for('new_investor', company_id=company['id']) }}">add a new one</a>.</div>
              {% endif %}
            </div>
            """,
            company=dict(company),
            investors=investors,
        )
        return render_page(body, subtitle=f"Investors — {company['name']}")

    @app.route("/companies/<int:company_id>/assign-investors", methods=["GET", "POST"])
    def assign_investors(company_id):
        """Bulk-assign/move investors into a company."""
        db = get_db()
        company = db.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        if not company:
            flash("Company not found.")
            return redirect(url_for("companies_list"))

        if request.method == "POST":
            ids = request.form.getlist("investor_ids")
            if ids:
                placeholders = ",".join("?" * len(ids))
                params = [company_id] + ids
                db.execute(f"UPDATE investors SET company_id = ? WHERE id IN ({placeholders})", params)
                db.commit()
                flash(f"Moved {len(ids)} investor(s) to {company['name']}.")
            else:
                flash("No investors selected.")
            return redirect(url_for("companies_list"))

        investors = db.execute(
            "SELECT id, full_name, company_id FROM investors ORDER BY full_name"
        ).fetchall()
        investors = [dict(r) for r in investors]
        body = render_template_string(
            """
            <div class="card">
              <div style="font-size:18px;font-weight:900">Assign Investors to {{ company['name'] }}</div>
              <div class="muted small" style="margin-bottom:12px">
                Select investors to move into this company. This updates their company association used for Suggested Investors.
              </div>
              <div class="hr"></div>
              {% if investors %}
              <form method="POST">
                <div style="max-height:500px;overflow-y:auto;margin-bottom:12px">
                  {% for inv in investors %}
                  <label style="display:flex;align-items:center;gap:8px;padding:4px 0;font-size:13px">
                    <input type="checkbox" name="investor_ids" value="{{ inv['id'] }}"
                           {% if inv['company_id'] == company['id'] %}checked{% endif %} />
                    <span>
                      <strong>{{ inv['full_name'] }}</strong>
                      {% if inv['company_id'] == company['id'] %}
                        <span class="muted small">(already in this company)</span>
                      {% endif %}
                    </span>
                  </label>
                  {% endfor %}
                </div>
                <div class="right">
                  <a class="btn" href="{{ url_for('companies_list') }}">Cancel</a>
                  <button class="btn primary" type="submit">Save Assignments</button>
                </div>
              </form>
              {% else %}
              <div class="muted">No investors in the system yet.</div>
              {% endif %}
            </div>
            """,
            company=dict(company),
            investors=investors,
        )
        return render_page(body, subtitle=f"Assign Investors — {company['name']}")

    @app.route("/companies/<int:company_id>/suggested-investors")
    def suggested_investors(company_id):
        db = get_db()
        top_n = request.args.get("topN", "25")
        try:
            top_n = min(200, max(1, int(top_n)))
        except (TypeError, ValueError):
            top_n = 25
        suggested_list, company_name, investor_count, connection_count, company_profile = run_pipeline(
            company_id, db, top_n=200
        )

        # Phase 2/3: enrich with fit_score, overlap_stats, relationship_strength
        for rec in suggested_list:
            candidate = {
                "name": rec["name"],
                "company": rec.get("company") or "",
                "position": rec.get("position") or "",
                "location": rec.get("location") or "",
                "linkedin_url": rec.get("linkedin_url") or "",
            }
            fit = compute_fit_score(
                company_id,
                candidate,
                company_profile,
                shared_investors_count=rec.get("shared_investors_count", 0),
                shared_org_count=rec.get("shared_org_count", 0),
            )
            rec["fit_score"] = fit["fit_score"]
            rec["fit_factors"] = fit.get("factors", [])
            rec["fit_breakdown"] = fit.get("breakdown", {})
            rec["overlap_stats"] = {
                "shared_investors_count": rec.get("shared_investors_count", 0),
                "shared_org_count": rec.get("shared_org_count", 0),
            }
            # Relationship strength between best connecting investor and candidate (if we have a source investor)
            if rec.get("source_investor_id"):
                rel = compute_investor_candidate_strength(
                    company_id,
                    rec["source_investor_id"],
                    candidate,
                    db,
                    shared_investors_count=rec.get("shared_investors_count", 0),
                    shared_org_count=rec.get("shared_org_count", 0),
                )
                rec["relationship_strength"] = rel.get("relationship_strength", 0)
                rec["relationship_factors"] = rel.get("factors", [])
            else:
                rec["relationship_strength"] = None
                rec["relationship_factors"] = []

        # Filters (query params)
        filter_industry = (request.args.get("industry") or "").strip().lower()
        filter_location = (request.args.get("location") or "").strip().lower()
        filter_firm_type = (request.args.get("firm_type") or "").strip().lower()
        filter_title = (request.args.get("title_pattern") or "").strip().lower()
        if filter_industry:
            suggested_list = [r for r in suggested_list if filter_industry in (r.get("position") or "").lower() or filter_industry in (r.get("company") or "").lower()]
        if filter_location:
            suggested_list = [r for r in suggested_list if filter_location in (r.get("location") or "").lower()]
        if filter_firm_type:
            suggested_list = [r for r in suggested_list if filter_firm_type in (r.get("company") or "").lower()]
        if filter_title:
            suggested_list = [r for r in suggested_list if filter_title in (r.get("position") or "").lower()]

        # Sort
        sort_by = request.args.get("sort", "relevance_score")
        if sort_by == "fit_score":
            suggested_list.sort(key=lambda x: (-x.get("fit_score", 0), -x.get("score", 0), x["name"].lower()))
        elif sort_by == "overlap":
            suggested_list.sort(key=lambda x: (-x.get("shared_investors_count", 0), -x.get("fit_score", 0), x["name"].lower()))
        elif sort_by == "location":
            suggested_list.sort(key=lambda x: ((x.get("location") or "").lower(), -x.get("fit_score", 0), x["name"].lower()))
        else:
            suggested_list.sort(key=lambda x: (-x.get("score", 0), -x.get("fit_score", 0), x["name"].lower()))

        # Apply topN (already limited in run_pipeline; re-slice after filter)
        suggested_list = suggested_list[:top_n]

        body = render_template_string(
            """
            <div class="card">
              <div style="font-size:18px;font-weight:900">Suggested Investors for {{ company_name }}</div>
              <div class="muted small" style="margin-bottom:8px">
                Based on your investors ({{ investor_count }}) and their connections ({{ connection_count }}). Multi-signal matches only. Fit score 0–100 + overlap.
              </div>
              <div class="hr"></div>
              <form method="GET" action="{{ url_for('suggested_investors', company_id=company_id) }}" style="margin-bottom:16px">
                <input type="hidden" name="company_id" value="{{ company_id }}" />
                <div class="row" style="flex-wrap:wrap;gap:10px;align-items:flex-end">
                  <div class="field" style="min-width:120px">
                    <label>Sort</label>
                    <select name="sort">
                      <option value="relevance_score" {{ 'selected' if sort_by == 'relevance_score' else '' }}>Relevance</option>
                      <option value="fit_score" {{ 'selected' if sort_by == 'fit_score' else '' }}>Fit Score</option>
                      <option value="overlap" {{ 'selected' if sort_by == 'overlap' else '' }}>Overlap</option>
                      <option value="location" {{ 'selected' if sort_by == 'location' else '' }}>Location</option>
                    </select>
                  </div>
                  <div class="field" style="min-width:100px">
                    <label>Top N</label>
                    <input type="number" name="topN" value="{{ top_n }}" min="1" max="200" style="width:70px" />
                  </div>
                  <div class="field" style="min-width:120px">
                    <label>Industry (filter)</label>
                    <input type="text" name="industry" value="{{ filter_industry }}" placeholder="e.g. SaaS" />
                  </div>
                  <div class="field" style="min-width:120px">
                    <label>Location (filter)</label>
                    <input type="text" name="location" value="{{ filter_location }}" placeholder="e.g. NYC" />
                  </div>
                  <div class="field" style="min-width:120px">
                    <label>Firm type (filter)</label>
                    <input type="text" name="firm_type" value="{{ filter_firm_type }}" placeholder="e.g. Capital" />
                  </div>
                  <div class="field" style="min-width:120px">
                    <label>Title pattern (filter)</label>
                    <input type="text" name="title_pattern" value="{{ filter_title }}" placeholder="e.g. Partner" />
                  </div>
                  <button type="submit" class="btn primary">Apply</button>
                </div>
              </form>
              {% if not suggested_list %}
              <div class="muted">
                No suggestions yet (or none match filters). Add investors and upload connection CSVs; recommendations require at least 2 signal categories.
              </div>
              {% else %}
              <div class="muted small">
                {{ suggested_list|length }} suggested investor(s). Fit score 0–100; overlap = how many investor networks they appear in.
              </div>
              {% endif %}
            </div>

            {% if suggested_list %}
            <div class="card" style="margin-top:20px">
              <div style="font-size:16px;font-weight:900;margin-bottom:12px">Suggested Investors</div>
              <div class="hr"></div>
              <div style="max-height:700px;overflow-y:auto">
                {% for rec in suggested_list %}
                <div style="padding:14px;margin-bottom:12px;background:#0c1322;border-radius:8px;border:1px solid var(--line)">
                  <div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:8px">
                    <div style="flex:1">
                      <div style="font-weight:700;font-size:15px;margin-bottom:4px">{{ rec.name }}</div>
                      {% if rec.position %}
                      <div class="muted small" style="margin-bottom:2px">{{ rec.position }}</div>
                      {% endif %}
                      {% if rec.company %}
                      <div class="muted small" style="margin-bottom:2px">🏢 {{ rec.company }}</div>
                      {% endif %}
                      {% if rec.location and rec.location.strip() %}
                      <div class="muted small">📍 {{ rec.location }}</div>
                      {% endif %}
                    </div>
                    <div style="text-align:right;min-width:180px">
                      <div class="pill" style="background:rgba(124,92,255,.2);border-color:#7c5cff;margin-bottom:4px">Fit: {{ rec.fit_score }}/100</div>
                      <div class="pill" style="background:rgba(34,197,94,.2);border-color:#22c55e;margin-bottom:4px">Score: {{ rec.score }}</div>
                      {% if rec.relationship_strength is not none %}
                      <div class="pill" style="font-size:10px;margin-bottom:4px">Relationship: {{ rec.relationship_strength }}/100</div>
                      {% endif %}
                      {% if rec.shared_investors_count and rec.shared_investors_count > 0 %}
                      <div class="pill" style="font-size:10px;margin-bottom:4px">Seen in {{ rec.shared_investors_count }} investor network{{ 's' if rec.shared_investors_count != 1 else '' }}</div>
                      {% endif %}
                      {% if rec.shared_org_count and rec.shared_org_count > 0 %}
                      <div class="pill" style="font-size:10px;margin-bottom:4px">Common org in network</div>
                      {% endif %}
                      <div style="display:flex;gap:4px;flex-wrap:wrap;justify-content:flex-end;margin-bottom:6px">
                        {% for key, label in signal_labels.items() %}
                        {% if rec.signals.get(key) %}
                        <span class="pill" style="font-size:10px;padding:4px 8px">{{ label }}</span>
                        {% endif %}
                        {% endfor %}
                      </div>
                      <div style="display:flex;gap:4px;flex-direction:column">
                        {% if rec.linkedin_url %}
                        <a href="{{ rec.linkedin_url }}" target="_blank" class="btn" style="padding:4px 8px;font-size:11px">LinkedIn</a>
                        {% endif %}
                        <a href="{{ url_for('new_investor', company_id=company_id, name=rec.name|urlencode, company=rec.company|urlencode, location=(rec.location or '')|urlencode) }}"
                           class="btn good" style="padding:4px 8px;font-size:11px">Add as Investor</a>
                      </div>
                    </div>
                  </div>
                  {% if rec.fit_factors %}
                  <div style="margin-top:6px;padding:6px;background:#070a0f;border-radius:6px">
                    <div style="font-size:11px;font-weight:600;margin-bottom:2px;color:#7c5cff">Fit factors:</div>
                    {% for f in rec.fit_factors[:6] %}
                    <div class="muted small" style="margin-left:6px;font-size:11px">• {{ f }}</div>
                    {% endfor %}
                  </div>
                  {% endif %}
                  {% if rec.relationship_factors %}
                  <div style="margin-top:6px;padding:6px;background:#070a0f;border-radius:6px">
                    <div style="font-size:11px;font-weight:600;margin-bottom:2px;color:#22c55e">Relationship factors:</div>
                    {% for rf in rec.relationship_factors[:3] %}
                    <div class="muted small" style="margin-left:6px;font-size:11px">• {{ rf }}</div>
                    {% endfor %}
                  </div>
                  {% endif %}
                  {% if rec.reasons %}
                  <div style="margin-top:6px;padding:6px;background:#070a0f;border-radius:6px">
                    <div style="font-size:11px;font-weight:600;margin-bottom:2px;color:#22c55e">Signals:</div>
                    {% for reason in rec.reasons %}
                    <div class="muted small" style="margin-left:6px;font-size:11px">• {{ reason }}</div>
                    {% endfor %}
                  </div>
                  {% endif %}
                </div>
                {% endfor %}
              </div>
            </div>
            {% endif %}
            """,
            company_name=company_name,
            company_id=company_id,
            investor_count=investor_count,
            connection_count=connection_count,
            suggested_list=suggested_list,
            signal_labels=SIGNAL_LABELS,
            sort_by=request.args.get("sort", "relevance_score"),
            top_n=top_n,
            filter_industry=filter_industry,
            filter_location=filter_location,
            filter_firm_type=filter_firm_type,
            filter_title=filter_title,
        )
        return render_page(body, subtitle=f"Suggested Investors — {company_name}")

    @app.route("/api/companies/<int:company_id>/solar-network/<int:node_id>")
    def api_solar_network_node(company_id, node_id):
        """API endpoint: Get node and its connections for solar system view."""
        db = get_db()
        result = get_node_connections(node_id, company_id, db)
        return jsonify(result)

    @app.route("/api/companies/<int:company_id>/solar-network/investors")
    def api_solar_network_investors(company_id):
        """API endpoint: Get all investors as starting points for solar system view."""
        db = get_db()
        investors = get_all_investors_for_solar(company_id, db)
        return jsonify({"investors": investors})

    @app.route("/api/companies/<int:company_id>/investor-overlap")
    def api_investor_overlap(company_id):
        """API endpoint: Get investor overlap matrix for chord diagram."""
        db = get_db()
        result = compute_investor_overlap_matrix(company_id, db)
        return jsonify(result)

    @app.route("/companies/<int:company_id>/access-map")
    def access_map(company_id):
        """Solar System Network visualization - infinite zoom navigation through connections."""
        db = get_db()
        company = db.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        if not company:
            flash("Company not found.")
            return redirect(url_for("companies_list"))
        company = dict(company)

        # Build or load access map (ensures data exists)
        try:
            result = build_access_map(company_id, db, store=True)
        except Exception:
            result = load_access_map(company_id, db)
        
        map_metrics = result.get("metrics", {}) or {}

        # Get initial investors for starting view
        investors = get_all_investors_for_solar(company_id, db)

        # Overlap intelligence for stats
        overlap = compute_overlap_intelligence(company_id, db)

        body = render_template_string(
            '''
            <style>
              .solar-container {
                position: relative;
                width: 100%;
                height: 560px;
                background: radial-gradient(ellipse at center, #0f1729 0%, #070a0f 100%);
                border-radius: 14px;
                overflow: hidden;
              }
              .solar-nav {
                position: absolute;
                top: 12px;
                left: 12px;
                z-index: 100;
                display: flex;
                gap: 8px;
                align-items: center;
              }
              .solar-nav .btn {
                padding: 8px 12px;
                font-size: 12px;
              }
              .solar-nav .btn:disabled {
                opacity: 0.4;
                cursor: not-allowed;
              }
              .solar-breadcrumb {
                display: flex;
                gap: 4px;
                align-items: center;
                font-size: 11px;
                color: var(--muted);
                flex-wrap: wrap;
                max-width: 400px;
              }
              .solar-breadcrumb-item {
                padding: 4px 8px;
                background: rgba(124, 92, 255, 0.2);
                border-radius: 4px;
                cursor: pointer;
                white-space: nowrap;
                max-width: 120px;
                overflow: hidden;
                text-overflow: ellipsis;
              }
              .solar-breadcrumb-item:hover {
                background: rgba(124, 92, 255, 0.4);
              }
              .solar-breadcrumb-sep {
                color: #3a4a66;
              }
              .solar-info {
                position: absolute;
                top: 12px;
                right: 12px;
                width: 250px;
                background: rgba(12, 19, 34, 0.95);
                border: 1px solid var(--line);
                border-radius: 8px;
                padding: 12px;
                font-size: 12px;
                z-index: 100;
                box-shadow: 0 4px 12px rgba(0,0,0,0.3);
              }
              .solar-info-title {
                font-weight: 700;
                font-size: 14px;
                margin-bottom: 4px;
              }
              .solar-info-meta {
                color: var(--muted);
              }
              .solar-loading {
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                color: var(--muted);
                font-size: 14px;
              }
              .node-investor { fill: #7c5cff; }
              .node-person { fill: #3b82f6; }
              .node-org { fill: #22c55e; }
              .node-glow-investor { stop-color: #7c5cff; }
              .node-glow-person { stop-color: #3b82f6; }
              .node-glow-org { stop-color: #22c55e; }
              
              /* Fullscreen styles */
              .fullscreen-mode {
                position: fixed !important;
                top: 0 !important;
                left: 0 !important;
                right: 0 !important;
                bottom: 0 !important;
                width: 100vw !important;
                height: 100vh !important;
                max-width: none !important;
                margin: 0 !important;
                border-radius: 0 !important;
                z-index: 9999 !important;
                background: #070a0f !important;
              }
              .fullscreen-mode .solar-container {
                height: calc(100vh - 120px) !important;
                border-radius: 0 !important;
              }
              .fullscreen-mode #connections-panel {
                max-height: 200px !important;
              }
              #fullscreen-btn.active {
                background: rgba(124, 92, 255, 0.5);
              }
            </style>

            <div class="card">
              <div style="font-size:18px;font-weight:900">Solar Network — {{ company['name'] }}</div>
              <div class="muted small" style="margin-bottom:12px">
                Click any node to zoom into their network. Use the back button or breadcrumbs to navigate.
              </div>
              <div class="hr"></div>
              <div class="kpi" style="margin-bottom:16px">
                <div class="card"><div class="n">{{ map_metrics.get('node_count', 0) }}</div><div class="t">Nodes</div></div>
                <div class="card"><div class="n">{{ map_metrics.get('edge_count', 0) }}</div><div class="t">Edges</div></div>
                <div class="card"><div class="n">{{ overlap.unique_people_count }}</div><div class="t">Unique People</div></div>
                <div class="card"><div class="n">{{ overlap.unique_org_count }}</div><div class="t">Unique Orgs</div></div>
                <div class="card"><div class="n">{{ overlap.overlap_people_count }}</div><div class="t">Overlap People</div></div>
                <div class="card"><div class="n">{{ overlap.overlap_percentage }}%</div><div class="t">Overlap %</div></div>
              </div>
            </div>

            <div class="card" style="margin-top:20px" id="network-explorer-card">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
                <div style="font-size:16px;font-weight:900">Network Explorer</div>
                <div style="display:flex;gap:8px">
                  <button class="btn" id="view-explore-btn" style="background:rgba(124,92,255,0.3)">Explore</button>
                  <button class="btn" id="view-overlap-btn">Overlap View</button>
                  <button class="btn" id="fullscreen-btn" title="Toggle Fullscreen">⛶ Fullscreen</button>
                </div>
              </div>
              <div class="hr"></div>
              <div class="solar-container" id="solar-container">
                <div class="solar-nav">
                  <button class="btn" id="solar-back-btn" disabled>← Back</button>
                  <button class="btn" id="solar-home-btn" disabled>Home</button>
                  <div class="solar-breadcrumb" id="solar-breadcrumb"></div>
                </div>
                <svg id="solar-svg" width="100%" height="100%"></svg>
                <div class="solar-loading" id="solar-loading">Loading network...</div>
                <div class="solar-info" id="solar-info" style="display:none">
                  <div class="solar-info-title" id="solar-info-title"></div>
                  <div class="solar-info-meta" id="solar-info-meta"></div>
                </div>
              </div>
              <div class="muted small" style="margin-top:8px" id="solar-help-text">
                Click any orbiting node to zoom into their connections. The center node shows the current focus.
              </div>
              
              <!-- All Connections Panel -->
              <div id="connections-panel" style="display:none;margin-top:12px;border-top:1px solid var(--line);padding-top:12px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                  <div style="font-weight:700;font-size:14px" id="connections-panel-title">All Connections</div>
                  <button class="btn" id="connections-panel-close" style="padding:4px 8px;font-size:11px">Close</button>
                </div>
                <div id="connections-panel-list" style="max-height:300px;overflow-y:auto;display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px;"></div>
              </div>
            </div>

            {% if overlap.top_overlapping_people or overlap.top_overlapping_orgs %}
            <div class="card" style="margin-top:20px">
              <div style="font-size:16px;font-weight:900;margin-bottom:12px">Top Overlapping People & Orgs</div>
              <div class="hr"></div>
              <div class="row" style="gap:24px">
                {% if overlap.top_overlapping_people %}
                <div style="flex:1;min-width:200px">
                  <div class="muted small" style="margin-bottom:8px">People in multiple investor networks</div>
                  <ul style="margin:0;padding-left:18px;font-size:13px">
                    {% for p in overlap.top_overlapping_people[:10] %}
                    <li>{{ p.label }} ({{ p.count }})</li>
                    {% endfor %}
                  </ul>
                </div>
                {% endif %}
                {% if overlap.top_overlapping_orgs %}
                <div style="flex:1;min-width:200px">
                  <div class="muted small" style="margin-bottom:8px">Orgs across multiple networks</div>
                  <ul style="margin:0;padding-left:18px;font-size:13px">
                    {% for o in overlap.top_overlapping_orgs[:10] %}
                    <li>{{ o.label }} ({{ o.count }})</li>
                    {% endfor %}
                  </ul>
                </div>
                {% endif %}
              </div>
            </div>
            {% endif %}

            <script src="https://d3js.org/d3.v7.min.js"></script>
            <script>
            (function() {
              const COMPANY_ID = {{ company_id }};
              const investors = {{ investors | tojson }};
              
              // State management
              let history = [];
              let currentCenter = null;
              let currentConnections = [];
              let isFullscreen = false;
              
              // DOM elements
              const container = document.getElementById('solar-container');
              const svg = d3.select('#solar-svg');
              const loadingEl = document.getElementById('solar-loading');
              const infoEl = document.getElementById('solar-info');
              const infoTitle = document.getElementById('solar-info-title');
              const infoMeta = document.getElementById('solar-info-meta');
              const backBtn = document.getElementById('solar-back-btn');
              const homeBtn = document.getElementById('solar-home-btn');
              const breadcrumbEl = document.getElementById('solar-breadcrumb');
              
              // Dynamic dimensions (recalculated on render)
              function getDimensions() {
                const w = container.clientWidth;
                const h = container.clientHeight;
                return {
                  width: w,
                  height: h,
                  centerX: w / 2,
                  centerY: h / 2,
                  orbitRadius: Math.min(w, h) * 0.35
                };
              }
              
              const CENTER_RADIUS = 50;
              const ORBIT_NODE_RADIUS_NORMAL = 24;
              const ORBIT_NODE_RADIUS_FULLSCREEN = 28;
              const MAX_VISIBLE_NODES_NORMAL = 24;
              const MAX_VISIBLE_NODES_FULLSCREEN = 85;
              
              // Get current settings based on fullscreen state
              function getMaxVisibleNodes() {
                return isFullscreen ? MAX_VISIBLE_NODES_FULLSCREEN : MAX_VISIBLE_NODES_NORMAL;
              }
              
              function getOrbitNodeRadius() {
                return isFullscreen ? ORBIT_NODE_RADIUS_FULLSCREEN : ORBIT_NODE_RADIUS_NORMAL;
              }
              
              // Colors by node type
              const nodeColors = {
                investor: '#7c5cff',
                person: '#3b82f6',
                org: '#22c55e'
              };
              
              // Create SVG defs for glow effects
              const defs = svg.append('defs');
              
              // Create glow filter for each type
              Object.entries(nodeColors).forEach(([type, color]) => {
                const filter = defs.append('filter')
                  .attr('id', `glow-${type}`)
                  .attr('x', '-50%')
                  .attr('y', '-50%')
                  .attr('width', '200%')
                  .attr('height', '200%');
                
                filter.append('feGaussianBlur')
                  .attr('stdDeviation', '4')
                  .attr('result', 'coloredBlur');
                
                const feMerge = filter.append('feMerge');
                feMerge.append('feMergeNode').attr('in', 'coloredBlur');
                feMerge.append('feMergeNode').attr('in', 'SourceGraphic');
              });
              
              // Create main group for transformations
              const mainGroup = svg.append('g').attr('class', 'main-group');
              
              // Create layer groups
              const edgesGroup = mainGroup.append('g').attr('class', 'edges-layer');
              const nodesGroup = mainGroup.append('g').attr('class', 'nodes-layer');
              
              // Update main group position based on current dimensions
              function updateMainGroupPosition() {
                const dim = getDimensions();
                mainGroup.attr('transform', `translate(${dim.centerX}, ${dim.centerY})`);
                return dim;
              }
              
              // Calculate orbital positions
              // Calculate orbital positions with multiple rings for many nodes
              function calculateOrbits(nodes, baseRadius, nodeRadius) {
                return calculateOrbitsScaled(nodes, baseRadius, nodeRadius, nodeRadius * 3);
              }
              
              // Calculate orbital positions with custom ring spacing
              function calculateOrbitsScaled(nodes, baseRadius, nodeRadius, ringSpacing) {
                const count = nodes.length;
                if (count === 0) return [];
                
                // Calculate how many nodes fit comfortably on one ring
                const minSpacing = nodeRadius * 2.8;
                const circumference = 2 * Math.PI * baseRadius;
                const nodesPerRing = Math.max(8, Math.floor(circumference / minSpacing));
                
                return nodes.map((node, i) => {
                  const ringIndex = Math.floor(i / nodesPerRing);
                  const posInRing = i % nodesPerRing;
                  const nodesInThisRing = Math.min(nodesPerRing, count - ringIndex * nodesPerRing);
                  
                  // Offset alternate rings for better spacing
                  const angleOffset = ringIndex % 2 === 1 ? Math.PI / nodesInThisRing : 0;
                  const ringRadius = baseRadius + ringIndex * ringSpacing;
                  const angle = 2 * Math.PI * posInRing / nodesInThisRing - Math.PI / 2 + angleOffset;
                  
                  return {
                    ...node,
                    x: ringRadius * Math.cos(angle),
                    y: ringRadius * Math.sin(angle),
                    ring: ringIndex,
                    nodeRadius: nodeRadius
                  };
                });
              }
              
              // Truncate label to fit inside node
              function truncateLabel(label, maxLen = 10) {
                if (!label) return '?';
                return label.length > maxLen ? label.substring(0, maxLen) + '..' : label;
              }
              
              // Update navigation UI
              function updateNavUI() {
                backBtn.disabled = history.length === 0;
                homeBtn.disabled = history.length === 0;
                
                // Update breadcrumb
                breadcrumbEl.innerHTML = '';
                
                if (history.length > 0) {
                  // Add home
                  const homeItem = document.createElement('span');
                  homeItem.className = 'solar-breadcrumb-item';
                  homeItem.textContent = 'Home';
                  homeItem.onclick = goHome;
                  breadcrumbEl.appendChild(homeItem);
                  
                  // Add history items (last 3)
                  const visibleHistory = history.slice(-3);
                  visibleHistory.forEach((node, idx) => {
                    const sep = document.createElement('span');
                    sep.className = 'solar-breadcrumb-sep';
                    sep.textContent = '›';
                    breadcrumbEl.appendChild(sep);
                    
                    const item = document.createElement('span');
                    item.className = 'solar-breadcrumb-item';
                    item.textContent = truncateLabel(node.label, 12);
                    item.title = node.label;
                    item.onclick = () => goToHistoryIndex(history.length - visibleHistory.length + idx);
                    breadcrumbEl.appendChild(item);
                  });
                }
                
                if (currentCenter) {
                  if (history.length > 0) {
                    const sep = document.createElement('span');
                    sep.className = 'solar-breadcrumb-sep';
                    sep.textContent = '›';
                    breadcrumbEl.appendChild(sep);
                  }
                  const current = document.createElement('span');
                  current.className = 'solar-breadcrumb-item';
                  current.style.background = 'rgba(124, 92, 255, 0.4)';
                  current.textContent = truncateLabel(currentCenter.label, 12);
                  current.title = currentCenter.label;
                  breadcrumbEl.appendChild(current);
                }
              }
              
              // Show info panel
              function showInfo(node) {
                infoTitle.textContent = node.label || '(unknown)';
                const meta = node.meta || {};
                let metaText = node.node_type ? `Type: ${node.node_type}` : '';
                if (meta.firm) metaText += ` • Firm: ${meta.firm}`;
                if (meta.title) metaText += ` • ${meta.title}`;
                if (meta.shared_investors_count) metaText += ` • Shared by ${meta.shared_investors_count} investor(s)`;
                infoMeta.textContent = metaText;
                infoEl.style.display = 'block';
              }
              
              // Hide info panel
              function hideInfo() {
                infoEl.style.display = 'none';
              }
              
              // Connections panel elements
              const connectionsPanel = document.getElementById('connections-panel');
              const connectionsPanelTitle = document.getElementById('connections-panel-title');
              const connectionsPanelList = document.getElementById('connections-panel-list');
              const connectionsPanelClose = document.getElementById('connections-panel-close');
              
              connectionsPanelClose.onclick = () => {
                connectionsPanel.style.display = 'none';
              };
              
              // Show all connections in panel
              function showAllConnections(center, connections) {
                connectionsPanelTitle.textContent = `All Connections for ${center.label} (${connections.length})`;
                
                connectionsPanelList.innerHTML = connections.map(conn => {
                  const meta = conn.meta || {};
                  const typeColor = conn.node_type === 'investor' ? '#7c5cff' : 
                                   conn.node_type === 'person' ? '#3b82f6' : '#22c55e';
                  return `
                    <div style="background:#0c1322;border:1px solid var(--line);border-radius:8px;padding:10px;cursor:pointer" 
                         class="connection-item" data-id="${conn.id}">
                      <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
                        <div style="width:8px;height:8px;border-radius:50%;background:${typeColor}"></div>
                        <div style="font-weight:600;font-size:13px">${conn.label || '(unknown)'}</div>
                      </div>
                      <div style="font-size:11px;color:var(--muted)">
                        ${meta.firm || meta.title || conn.node_type || ''}
                      </div>
                    </div>
                  `;
                }).join('');
                
                // Add click handlers to connection items
                connectionsPanel.querySelectorAll('.connection-item').forEach(item => {
                  item.onclick = () => {
                    const nodeId = parseInt(item.dataset.id);
                    const node = connections.find(c => c.id === nodeId);
                    if (node) {
                      connectionsPanel.style.display = 'none';
                      zoomToNode(node);
                    }
                  };
                });
                
                connectionsPanel.style.display = 'block';
              }
              
              // Render the solar system view
              function render(center, connections, animate = true) {
                loadingEl.style.display = 'none';
                currentCenter = center;
                currentConnections = connections;
                
                // Update center position for current container size
                const dim = updateMainGroupPosition();
                const ORBIT_NODE_RADIUS = getOrbitNodeRadius();
                
                // Limit visible nodes (more in fullscreen mode)
                const maxNodes = getMaxVisibleNodes();
                const visibleConnections = connections.slice(0, maxNodes);
                const hiddenCount = connections.length - visibleConnections.length;
                
                // Calculate how many rings we need and adjust orbit radius to fit
                const minSpacing = ORBIT_NODE_RADIUS * 2.8;
                const baseOrbitRadius = dim.orbitRadius;
                const circumference = 2 * Math.PI * baseOrbitRadius;
                const nodesPerRing = Math.max(8, Math.floor(circumference / minSpacing));
                const numRingsNeeded = Math.ceil(visibleConnections.length / nodesPerRing);
                const ringSpacing = ORBIT_NODE_RADIUS * 3;
                
                // Calculate total space needed and available
                const totalRadiusNeeded = baseOrbitRadius + (numRingsNeeded - 1) * ringSpacing + ORBIT_NODE_RADIUS + 40;
                const availableRadius = Math.min(dim.width, dim.height) / 2 - 20; // Leave 20px margin
                
                // Scale down if needed to fit all rings
                const scaleFactor = totalRadiusNeeded > availableRadius ? availableRadius / totalRadiusNeeded : 1;
                const ORBIT_RADIUS = baseOrbitRadius * scaleFactor;
                const adjustedRingSpacing = ringSpacing * scaleFactor;
                const adjustedNodeRadius = Math.max(16, ORBIT_NODE_RADIUS * scaleFactor);
                
                // Calculate positions with multi-ring support (pass adjusted values)
                const orbitNodes = calculateOrbitsScaled(visibleConnections, ORBIT_RADIUS, adjustedNodeRadius, adjustedRingSpacing);
                
                // Clear existing
                edgesGroup.selectAll('*').remove();
                nodesGroup.selectAll('*').remove();
                
                // Draw edges (connections from center to orbit nodes)
                const edges = edgesGroup.selectAll('.edge')
                  .data(orbitNodes)
                  .enter()
                  .append('line')
                  .attr('class', 'edge')
                  .attr('x1', 0)
                  .attr('y1', 0)
                  .attr('x2', d => d.x)
                  .attr('y2', d => d.y)
                  .attr('stroke', '#243043')
                  .attr('stroke-width', 1)
                  .style('opacity', animate ? 0 : 0.6);
                
                if (animate) {
                  edges.transition()
                    .duration(400)
                    .delay((d, i) => 200 + i * 30)
                    .style('opacity', 0.6);
                }
                
                // Draw orbit paths (decorative) - one for each ring
                const numRings = orbitNodes.length > 0 ? Math.max(...orbitNodes.map(n => n.ring || 0)) + 1 : 1;
                
                for (let ring = 0; ring < numRings; ring++) {
                  nodesGroup.append('circle')
                    .attr('cx', 0)
                    .attr('cy', 0)
                    .attr('r', ORBIT_RADIUS + ring * adjustedRingSpacing)
                    .attr('fill', 'none')
                    .attr('stroke', '#1a2333')
                    .attr('stroke-width', 1)
                    .attr('stroke-dasharray', '4,4')
                    .style('opacity', animate ? 0 : 0.4)
                    .transition()
                    .duration(400)
                    .style('opacity', 0.4);
                }
                
                // Draw center node (sun)
                const centerGroup = nodesGroup.append('g')
                  .attr('class', 'center-node')
                  .style('cursor', 'default');
                
                centerGroup.append('circle')
                  .attr('cx', 0)
                  .attr('cy', 0)
                  .attr('r', animate ? 0 : CENTER_RADIUS)
                  .attr('fill', nodeColors[center.node_type] || '#7c5cff')
                  .attr('filter', `url(#glow-${center.node_type || 'investor'})`)
                  .transition()
                  .duration(animate ? 600 : 0)
                  .ease(d3.easeElasticOut.amplitude(1).period(0.5))
                  .attr('r', CENTER_RADIUS);
                
                centerGroup.append('text')
                  .attr('x', 0)
                  .attr('y', 4)
                  .attr('text-anchor', 'middle')
                  .attr('fill', '#fff')
                  .attr('font-size', '11px')
                  .attr('font-weight', '700')
                  .text(truncateLabel(center.label, 12))
                  .style('opacity', animate ? 0 : 1)
                  .transition()
                  .duration(400)
                  .delay(300)
                  .style('opacity', 1);
                
                centerGroup
                  .style('cursor', 'pointer')
                  .on('mouseover', () => showInfo(center))
                  .on('mouseout', hideInfo)
                  .on('click', () => {
                    if (connections.length > 0) {
                      showAllConnections(center, connections);
                    }
                  });
                
                // Draw orbit nodes (planets)
                const orbitNodeGroups = nodesGroup.selectAll('.orbit-node')
                  .data(orbitNodes)
                  .enter()
                  .append('g')
                  .attr('class', 'orbit-node')
                  .attr('transform', d => `translate(${animate ? 0 : d.x}, ${animate ? 0 : d.y})`)
                  .style('cursor', 'pointer');
                
                // Use initials or very short names inside nodes
                const fontSize = 9;
                
                // Function to get short display text (initials or first name)
                function getNodeDisplayText(label) {
                  if (!label) return '?';
                  const words = label.trim().split(/\s+/);
                  if (words.length >= 2) {
                    // Show initials for multi-word names
                    return words.map(w => w.charAt(0).toUpperCase()).slice(0, 3).join('');
                  }
                  // Single word - show first 5 chars
                  return label.substring(0, 5);
                }
                
                orbitNodeGroups.append('circle')
                  .attr('r', d => animate ? 0 : (d.nodeRadius || adjustedNodeRadius))
                  .attr('fill', d => nodeColors[d.node_type] || '#3b82f6')
                  .attr('stroke', '#0c1322')
                  .attr('stroke-width', 2);
                
                // Initials inside the node
                orbitNodeGroups.append('text')
                  .attr('class', 'node-initials')
                  .attr('y', 3)
                  .attr('text-anchor', 'middle')
                  .attr('fill', '#fff')
                  .attr('font-size', `${fontSize}px`)
                  .attr('font-weight', '700')
                  .text(d => getNodeDisplayText(d.label))
                  .style('pointer-events', 'none')
                  .style('opacity', animate ? 0 : 1);
                
                // Animate orbit nodes
                if (animate) {
                  orbitNodeGroups.transition()
                    .duration(600)
                    .delay((d, i) => 200 + i * 40)
                    .ease(d3.easeBackOut.overshoot(1.2))
                    .attr('transform', d => `translate(${d.x}, ${d.y})`);
                  
                  orbitNodeGroups.select('circle')
                    .transition()
                    .duration(400)
                    .delay((d, i) => 300 + i * 40)
                    .attr('r', d => d.nodeRadius || adjustedNodeRadius);
                  
                  orbitNodeGroups.select('.node-initials')
                    .transition()
                    .duration(300)
                    .delay((d, i) => 500 + i * 40)
                    .style('opacity', 1);
                }
                
                // Add interactions
                orbitNodeGroups
                  .on('mouseover', function(event, d) {
                    const node = d3.select(this);
                    const nodeR = d.nodeRadius || adjustedNodeRadius;
                    
                    // Bring this node to front (raise it above other nodes)
                    this.parentNode.appendChild(this);
                    
                    node.select('circle')
                      .transition()
                      .duration(150)
                      .attr('r', nodeR + 4);
                    
                    // Show full name label near the node with background
                    const labelText = d.label || '(unknown)';
                    const labelWidth = labelText.length * 7 + 16;
                    const labelHeight = 22;
                    
                    // Background pill for label
                    node.append('rect')
                      .attr('class', 'hover-label-bg')
                      .attr('x', -labelWidth / 2)
                      .attr('y', -nodeR - labelHeight - 6)
                      .attr('width', labelWidth)
                      .attr('height', labelHeight)
                      .attr('rx', labelHeight / 2)
                      .attr('fill', 'rgba(12, 19, 34, 0.95)')
                      .attr('stroke', '#3b82f6')
                      .attr('stroke-width', 1);
                    
                    // Label text
                    node.append('text')
                      .attr('class', 'hover-label')
                      .attr('y', -nodeR - labelHeight / 2 - 3)
                      .attr('text-anchor', 'middle')
                      .attr('fill', '#fff')
                      .attr('font-size', '11px')
                      .attr('font-weight', '600')
                      .text(labelText);
                    
                    showInfo(d);
                  })
                  .on('mouseout', function(event, d) {
                    const node = d3.select(this);
                    const nodeR = d.nodeRadius || adjustedNodeRadius;
                    node.select('circle')
                      .transition()
                      .duration(150)
                      .attr('r', nodeR);
                    
                    // Remove hover label and background
                    node.select('.hover-label').remove();
                    node.select('.hover-label-bg').remove();
                    
                    hideInfo();
                  })
                  .on('click', function(event, d) {
                    zoomToNode(d);
                  });
                
                // Show "+N more" indicator if there are hidden nodes (clickable)
                if (hiddenCount > 0) {
                  // Position at bottom center, below the outermost ring
                  const outerRingRadius = ORBIT_RADIUS + (numRings - 1) * adjustedRingSpacing;
                  const moreGroup = nodesGroup.append('g')
                    .attr('class', 'more-indicator')
                    .attr('transform', `translate(0, ${outerRingRadius + adjustedNodeRadius + 25})`)
                    .style('cursor', 'pointer');
                  
                  // Background pill
                  const pillWidth = 120;
                  const pillHeight = 32;
                  moreGroup.append('rect')
                    .attr('x', -pillWidth/2)
                    .attr('y', -pillHeight/2)
                    .attr('width', pillWidth)
                    .attr('height', pillHeight)
                    .attr('fill', 'rgba(124, 92, 255, 0.3)')
                    .attr('stroke', '#7c5cff')
                    .attr('stroke-width', 1)
                    .attr('rx', pillHeight/2)
                    .style('opacity', 0)
                    .transition()
                    .duration(400)
                    .delay(600)
                    .style('opacity', 1);
                  
                  // Text
                  moreGroup.append('text')
                    .attr('text-anchor', 'middle')
                    .attr('dominant-baseline', 'middle')
                    .attr('fill', '#fff')
                    .attr('font-size', '13px')
                    .attr('font-weight', '600')
                    .text(`+${hiddenCount} more`)
                    .style('opacity', 0)
                    .transition()
                    .duration(400)
                    .delay(600)
                    .style('opacity', 1);
                  
                  // Hover effect
                  moreGroup
                    .on('mouseover', function() {
                      d3.select(this).select('rect')
                        .transition()
                        .duration(150)
                        .attr('fill', 'rgba(124, 92, 255, 0.5)');
                    })
                    .on('mouseout', function() {
                      d3.select(this).select('rect')
                        .transition()
                        .duration(150)
                        .attr('fill', 'rgba(124, 92, 255, 0.3)');
                    })
                    .on('click', () => showAllConnections(center, connections));
                }
                
                updateNavUI();
              }
              
              // Fetch node connections from API
              async function fetchNodeConnections(nodeId) {
                loadingEl.style.display = 'block';
                try {
                  const response = await fetch(`/api/companies/${COMPANY_ID}/solar-network/${nodeId}`);
                  const data = await response.json();
                  return data;
                } catch (error) {
                  console.error('Failed to fetch node connections:', error);
                  loadingEl.textContent = 'Failed to load network data';
                  return null;
                }
              }
              
              // Zoom to a specific node
              async function zoomToNode(node) {
                if (currentCenter) {
                  history.push(currentCenter);
                }
                
                // Animate out current view
                nodesGroup.selectAll('.orbit-node')
                  .transition()
                  .duration(300)
                  .attr('transform', 'translate(0, 0)')
                  .style('opacity', 0);
                
                nodesGroup.selectAll('.center-node')
                  .transition()
                  .duration(300)
                  .style('opacity', 0);
                
                edgesGroup.selectAll('.edge')
                  .transition()
                  .duration(200)
                  .style('opacity', 0);
                
                // Fetch new data
                const data = await fetchNodeConnections(node.id);
                if (data && data.center) {
                  setTimeout(() => {
                    render(data.center, data.connections || [], true);
                  }, 350);
                }
              }
              
              // Go back in history
              function goBack() {
                if (history.length === 0) return;
                
                const previousNode = history.pop();
                
                // Animate out
                nodesGroup.selectAll('*')
                  .transition()
                  .duration(200)
                  .style('opacity', 0);
                
                edgesGroup.selectAll('*')
                  .transition()
                  .duration(200)
                  .style('opacity', 0);
                
                // Check if target is the virtual company center (id === 'company')
                if (previousNode.id === 'company') {
                  setTimeout(() => {
                    history = [];
                    renderInitialView();
                  }, 250);
                  return;
                }
                
                // Fetch and render previous node
                fetchNodeConnections(previousNode.id).then(data => {
                  if (data && data.center) {
                    setTimeout(() => {
                      render(data.center, data.connections || [], true);
                    }, 250);
                  } else {
                    // Fallback: re-render with cached node data
                    setTimeout(() => {
                      loadingEl.style.display = 'none';
                      render(previousNode, [], true);
                    }, 250);
                  }
                });
              }
              
              // Go to home (initial view)
              function goHome() {
                // Animate out current view
                nodesGroup.selectAll('*')
                  .transition()
                  .duration(200)
                  .style('opacity', 0);
                
                edgesGroup.selectAll('*')
                  .transition()
                  .duration(200)
                  .style('opacity', 0);
                
                setTimeout(() => {
                  history = [];
                  renderInitialView();
                }, 250);
              }
              
              // Go to specific history index
              function goToHistoryIndex(index) {
                if (index < 0 || index >= history.length) return;
                
                const targetNode = history[index];
                history = history.slice(0, index);
                
                // Animate out
                nodesGroup.selectAll('*')
                  .transition()
                  .duration(200)
                  .style('opacity', 0);
                
                edgesGroup.selectAll('*')
                  .transition()
                  .duration(200)
                  .style('opacity', 0);
                
                // Check if target is the virtual company center (id === 'company')
                if (targetNode.id === 'company') {
                  setTimeout(() => {
                    history = [];
                    renderInitialView();
                  }, 250);
                  return;
                }
                
                // Fetch and render target node
                fetchNodeConnections(targetNode.id).then(data => {
                  if (data && data.center) {
                    setTimeout(() => {
                      render(data.center, data.connections || [], true);
                    }, 250);
                  } else {
                    // Fallback: re-render the target node with its cached data
                    setTimeout(() => {
                      loadingEl.style.display = 'none';
                      render(targetNode, [], true);
                    }, 250);
                  }
                });
              }
              
              // Render initial view with investors
              function renderInitialView() {
                if (investors.length === 0) {
                  loadingEl.textContent = 'No investors found. Add investors and upload connections first.';
                  return;
                }
                
                // If only one investor, zoom into them directly
                if (investors.length === 1) {
                  fetchNodeConnections(investors[0].id).then(data => {
                    if (data && data.center) {
                      render(data.center, data.connections || [], true);
                    }
                  });
                  return;
                }
                
                // Multiple investors: show them as the "home" view
                // Create a virtual center node representing the company
                const virtualCenter = {
                  id: 'company',
                  label: '{{ company["name"] }}',
                  node_type: 'investor',
                  meta: { title: 'Your Company' }
                };
                
                render(virtualCenter, investors, true);
              }
              
              // View mode: 'explore' or 'overlap'
              let viewMode = 'explore';
              const exploreBtn = document.getElementById('view-explore-btn');
              const overlapBtn = document.getElementById('view-overlap-btn');
              const helpText = document.getElementById('solar-help-text');
              
              // Overlap data cache
              let overlapData = null;
              
              // Event listeners
              backBtn.onclick = goBack;
              homeBtn.onclick = goHome;
              
              exploreBtn.onclick = () => {
                if (viewMode === 'explore') return;
                viewMode = 'explore';
                exploreBtn.style.background = 'rgba(124,92,255,0.3)';
                overlapBtn.style.background = '';
                helpText.textContent = 'Click any orbiting node to zoom into their connections. The center node shows the current focus.';
                backBtn.style.display = '';
                homeBtn.style.display = '';
                breadcrumbEl.style.display = '';
                history = [];
                renderInitialView();
              };
              
              overlapBtn.onclick = () => {
                if (viewMode === 'overlap') return;
                viewMode = 'overlap';
                overlapBtn.style.background = 'rgba(124,92,255,0.3)';
                exploreBtn.style.background = '';
                helpText.textContent = 'Shows all investors and their shared connections. Hover over shared connections to see which investors share them.';
                backBtn.style.display = 'none';
                homeBtn.style.display = 'none';
                breadcrumbEl.style.display = 'none';
                renderOverlapView();
              };
              
              // Fullscreen toggle
              const fullscreenBtn = document.getElementById('fullscreen-btn');
              const explorerCard = document.getElementById('network-explorer-card');
              
              fullscreenBtn.onclick = () => {
                isFullscreen = !isFullscreen;
                if (isFullscreen) {
                  explorerCard.classList.add('fullscreen-mode');
                  fullscreenBtn.classList.add('active');
                  fullscreenBtn.textContent = '✕ Exit Fullscreen';
                  document.body.style.overflow = 'hidden';
                } else {
                  explorerCard.classList.remove('fullscreen-mode');
                  fullscreenBtn.classList.remove('active');
                  fullscreenBtn.textContent = '⛶ Fullscreen';
                  document.body.style.overflow = '';
                }
                // Re-render to adjust to new size
                if (viewMode === 'explore') {
                  if (currentCenter && currentConnections) {
                    render(currentCenter, currentConnections, false);
                  }
                } else {
                  renderOverlapView();
                }
              };
              
              // ESC key to exit fullscreen
              document.addEventListener('keydown', (e) => {
                if (e.key === 'Escape' && isFullscreen) {
                  fullscreenBtn.click();
                }
              });
              
              // Fetch overlap data
              async function fetchOverlapData() {
                if (overlapData) return overlapData;
                try {
                  const response = await fetch(`/api/companies/${COMPANY_ID}/investor-overlap`);
                  overlapData = await response.json();
                  return overlapData;
                } catch (err) {
                  console.error('Failed to fetch overlap data:', err);
                  return null;
                }
              }
              
              // Render overlap view - shows investors around edge with shared connections in center
              async function renderOverlapView() {
                loadingEl.style.display = 'block';
                loadingEl.textContent = 'Loading overlap data...';
                
                const data = await fetchOverlapData();
                if (!data || !data.investors || data.investors.length < 2) {
                  loadingEl.textContent = 'Need at least 2 investors to show overlap.';
                  return;
                }
                
                loadingEl.style.display = 'none';
                
                // Update center position for current container size
                const dim = updateMainGroupPosition();
                
                const { investors: invList, matrix, shared_connections } = data;
                const n = invList.length;
                
                // Clear existing
                edgesGroup.selectAll('*').remove();
                nodesGroup.selectAll('*').remove();
                
                // Investor colors
                const invColors = ['#7c5cff', '#3b82f6', '#22c55e', '#f59e0b', '#ef4444'];
                
                // Position investors in a circle around the edge
                const OUTER_RADIUS = Math.min(dim.width, dim.height) * 0.38;
                const INVESTOR_RADIUS = 35;
                const invPositions = invList.map((inv, i) => ({
                  ...inv,
                  idx: i,
                  x: OUTER_RADIUS * Math.cos(2 * Math.PI * i / n - Math.PI / 2),
                  y: OUTER_RADIUS * Math.sin(2 * Math.PI * i / n - Math.PI / 2),
                  color: invColors[i % invColors.length]
                }));
                
                // Collect all shared people with their investor connections
                const sharedPeople = [];
                for (const [key, people] of Object.entries(shared_connections)) {
                  const [i, j] = key.split('-').map(Number);
                  people.forEach((p, idx) => {
                    if (idx < 5) { // Limit to 5 per pair for clarity
                      sharedPeople.push({
                        ...p,
                        investors: [i, j],
                        pairKey: key
                      });
                    }
                  });
                }
                
                // Position shared people in center area
                const INNER_RADIUS = Math.min(dim.width, dim.height) * 0.18;
                const sharedPositions = sharedPeople.map((p, i) => {
                  const angle = 2 * Math.PI * i / sharedPeople.length;
                  const radius = INNER_RADIUS * (0.5 + Math.random() * 0.5);
                  return {
                    ...p,
                    x: radius * Math.cos(angle),
                    y: radius * Math.sin(angle)
                  };
                });
                
                // Draw connections from shared people to their investors
                sharedPositions.forEach(sp => {
                  sp.investors.forEach(invIdx => {
                    const inv = invPositions[invIdx];
                    edgesGroup.append('line')
                      .attr('class', `overlap-edge inv-${invIdx}`)
                      .attr('x1', sp.x)
                      .attr('y1', sp.y)
                      .attr('x2', inv.x)
                      .attr('y2', inv.y)
                      .attr('stroke', inv.color)
                      .attr('stroke-width', 1)
                      .attr('stroke-opacity', 0.3);
                  });
                });
                
                // Draw investor nodes (outer ring)
                const invGroups = nodesGroup.selectAll('.investor-node')
                  .data(invPositions)
                  .enter()
                  .append('g')
                  .attr('class', 'investor-node')
                  .attr('transform', d => `translate(${d.x}, ${d.y})`);
                
                invGroups.append('circle')
                  .attr('r', INVESTOR_RADIUS)
                  .attr('fill', d => d.color)
                  .attr('stroke', '#0c1322')
                  .attr('stroke-width', 3)
                  .attr('filter', d => `url(#glow-investor)`);
                
                invGroups.append('text')
                  .attr('y', 4)
                  .attr('text-anchor', 'middle')
                  .attr('fill', '#fff')
                  .attr('font-size', '10px')
                  .attr('font-weight', '700')
                  .text(d => truncateLabel(d.full_name, 10));
                
                invGroups
                  .on('mouseover', function(event, d) {
                    // Highlight edges for this investor
                    edgesGroup.selectAll('.overlap-edge')
                      .attr('stroke-opacity', 0.1);
                    edgesGroup.selectAll(`.inv-${d.idx}`)
                      .attr('stroke-opacity', 0.8)
                      .attr('stroke-width', 2);
                    
                    showInfo({
                      label: d.full_name,
                      node_type: 'investor',
                      meta: { firm: d.firm, title: `${d.connection_count} connections` }
                    });
                  })
                  .on('mouseout', function() {
                    edgesGroup.selectAll('.overlap-edge')
                      .attr('stroke-opacity', 0.3)
                      .attr('stroke-width', 1);
                    hideInfo();
                  });
                
                // Draw shared people nodes (center area)
                const sharedGroups = nodesGroup.selectAll('.shared-node')
                  .data(sharedPositions)
                  .enter()
                  .append('g')
                  .attr('class', 'shared-node')
                  .attr('transform', d => `translate(${d.x}, ${d.y})`);
                
                sharedGroups.append('circle')
                  .attr('r', 12)
                  .attr('fill', '#3b82f6')
                  .attr('stroke', '#0c1322')
                  .attr('stroke-width', 2);
                
                sharedGroups.append('text')
                  .attr('y', 3)
                  .attr('text-anchor', 'middle')
                  .attr('fill', '#fff')
                  .attr('font-size', '7px')
                  .attr('font-weight', '600')
                  .text(d => {
                    const parts = d.name.split(' ');
                    return parts.length >= 2 ? parts[0][0] + parts[1][0] : d.name.substring(0, 2);
                  });
                
                sharedGroups
                  .on('mouseover', function(event, d) {
                    d3.select(this).select('circle')
                      .transition()
                      .duration(150)
                      .attr('r', 16);
                    
                    // Highlight edges to connected investors
                    edgesGroup.selectAll('.overlap-edge')
                      .attr('stroke-opacity', 0.1);
                    d.investors.forEach(invIdx => {
                      edgesGroup.selectAll(`.inv-${invIdx}`)
                        .filter(function() {
                          const line = d3.select(this);
                          return Math.abs(parseFloat(line.attr('x1')) - d.x) < 1 && 
                                 Math.abs(parseFloat(line.attr('y1')) - d.y) < 1;
                        })
                        .attr('stroke-opacity', 1)
                        .attr('stroke-width', 2);
                    });
                    
                    const invNames = d.investors.map(i => invPositions[i].full_name).join(' & ');
                    showInfo({
                      label: d.name,
                      node_type: 'person',
                      meta: { 
                        firm: d.company,
                        title: `Shared by: ${invNames}`
                      }
                    });
                  })
                  .on('mouseout', function() {
                    d3.select(this).select('circle')
                      .transition()
                      .duration(150)
                      .attr('r', 12);
                    
                    edgesGroup.selectAll('.overlap-edge')
                      .attr('stroke-opacity', 0.3)
                      .attr('stroke-width', 1);
                    
                    hideInfo();
                  });
                
                // Add center label
                nodesGroup.append('text')
                  .attr('x', 0)
                  .attr('y', 0)
                  .attr('text-anchor', 'middle')
                  .attr('fill', '#6b7280')
                  .attr('font-size', '10px')
                  .text(sharedPeople.length > 0 ? `${sharedPeople.length} shared` : 'No shared connections');
                
                updateNavUI();
              }
              
              // Handle window resize - recenter the visualization
              let resizeTimeout;
              window.addEventListener('resize', () => {
                clearTimeout(resizeTimeout);
                resizeTimeout = setTimeout(() => {
                  if (viewMode === 'explore') {
                    if (currentCenter && currentConnections) {
                      render(currentCenter, currentConnections, false);
                    }
                  } else {
                    renderOverlapView();
                  }
                }, 100);
              });
              
              // Initialize
              updateMainGroupPosition();
              renderInitialView();
            })();
            </script>
            ''',
            company=company,
            company_id=company_id,
            map_metrics=map_metrics,
            overlap=overlap,
            investors=investors,
        )
        return render_page(body, subtitle=f"Solar Network — {company['name']}")
