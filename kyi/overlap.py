"""
Phase 2: Overlap Intelligence per company.
- total_nodes, total_edges, unique_people_count, unique_org_count
- overlap_people_count, overlap_org_count, overlap_percentage
- top_overlapping_people, top_overlapping_orgs
- collapse_rate: second-degree becoming first-degree (candidates in >=2 investor networks)
"""
from collections import defaultdict


def compute_overlap_intelligence(company_id: int, db) -> dict:
    """
    Compute overlap analytics for a company from its investors and connections.
    Returns dict: total_nodes, total_edges, unique_people_count, unique_org_count,
    overlap_people_count, overlap_org_count, overlap_percentage,
    top_overlapping_people (top 20), top_overlapping_orgs (top 20),
    collapse_rate (candidates appearing in >=2 investors' networks).
    """
    investors = db.execute(
        "SELECT id FROM investors WHERE company_id = ?",
        (company_id,),
    ).fetchall()
    investor_ids = [r["id"] for r in investors]
    if not investor_ids:
        return _empty_overlap_metrics()

    placeholders = ",".join("?" * len(investor_ids))
    connections = db.execute(
        f"""
        SELECT id, investor_id, full_name, first_name, last_name, company
        FROM connections
        WHERE investor_id IN ({placeholders})
        AND (full_name IS NOT NULL AND full_name != '')
        """,
        tuple(investor_ids),
    ).fetchall()

    # Person key: normalized full_name (or first+last)
    def person_key(r):
        fn = (r["full_name"] or "").strip()
        if not fn:
            fn = f"{(r['first_name'] or '').strip()} {(r['last_name'] or '').strip()}".strip()
        return fn.lower() if fn else None

    # Org key: company name lower
    def org_key(r):
        c = (r["company"] or "").strip()
        return c.lower() if c else None

    person_to_investors = defaultdict(set)  # person_key -> set(investor_id)
    org_to_investors = defaultdict(set)
    all_people = set()
    all_orgs = set()

    for r in connections:
        pk = person_key(r)
        if pk:
            all_people.add(pk)
            person_to_investors[pk].add(r["investor_id"])
        ok = org_key(r)
        if ok:
            all_orgs.add(ok)
            org_to_investors[ok].add(r["investor_id"])

    unique_people_count = len(all_people)
    unique_org_count = len(all_orgs)
    overlap_people = {k: v for k, v in person_to_investors.items() if len(v) >= 2}
    overlap_orgs = {k: v for k, v in org_to_investors.items() if len(v) >= 2}
    overlap_people_count = len(overlap_people)
    overlap_org_count = len(overlap_orgs)

    total_unique = unique_people_count + unique_org_count
    total_overlap = overlap_people_count + overlap_org_count
    overlap_percentage = (total_overlap / total_unique * 100) if total_unique else 0.0

    # Top 20 overlapping people (by number of investor networks)
    top_people = sorted(
        [(k, len(v)) for k, v in overlap_people.items()],
        key=lambda x: -x[1],
    )[:20]
    top_overlapping_people = [{"label": k, "count": v} for k, v in top_people]

    top_orgs = sorted(
        [(k, len(v)) for k, v in overlap_orgs.items()],
        key=lambda x: -x[1],
    )[:20]
    top_overlapping_orgs = [{"label": k, "count": v} for k, v in top_orgs]

    # Collapse rate: people appearing in >=2 investors' networks (second-degree -> first-degree)
    collapse_count = overlap_people_count
    collapse_rate = (collapse_count / unique_people_count * 100) if unique_people_count else 0.0

    # total_nodes: investors + unique people + unique orgs (simplified for metrics)
    total_nodes = len(investor_ids) + unique_people_count + unique_org_count
    # total_edges: investor->connection (direct) per connection row
    total_edges = len(connections)

    return {
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "unique_people_count": unique_people_count,
        "unique_org_count": unique_org_count,
        "overlap_people_count": overlap_people_count,
        "overlap_org_count": overlap_org_count,
        "overlap_percentage": round(overlap_percentage, 1),
        "top_overlapping_people": top_overlapping_people,
        "top_overlapping_orgs": top_overlapping_orgs,
        "collapse_count": collapse_count,
        "collapse_rate": round(collapse_rate, 1),
        "person_to_investors": person_to_investors,
        "org_to_investors": org_to_investors,
    }


def _empty_overlap_metrics():
    return {
        "total_nodes": 0,
        "total_edges": 0,
        "unique_people_count": 0,
        "unique_org_count": 0,
        "overlap_people_count": 0,
        "overlap_org_count": 0,
        "overlap_percentage": 0.0,
        "top_overlapping_people": [],
        "top_overlapping_orgs": [],
        "collapse_count": 0,
        "collapse_rate": 0.0,
        "person_to_investors": {},
        "org_to_investors": {},
    }


def compute_investor_overlap_matrix(company_id: int, db) -> dict:
    """
    Compute overlap matrix between all investors in a company.
    Returns: {
        investors: [{id, name, connection_count}],
        matrix: [[overlap_count between i and j]],
        shared_connections: {(inv_i, inv_j): [{name, company}]}
    }
    """
    investors = db.execute(
        "SELECT id, full_name, firm FROM investors WHERE company_id = ?",
        (company_id,),
    ).fetchall()
    investors = [dict(r) for r in investors]
    
    if len(investors) < 2:
        return {"investors": investors, "matrix": [], "shared_connections": {}}
    
    investor_ids = [inv["id"] for inv in investors]
    inv_id_to_idx = {inv_id: idx for idx, inv_id in enumerate(investor_ids)}
    
    placeholders = ",".join("?" * len(investor_ids))
    connections = db.execute(
        f"""
        SELECT id, investor_id, full_name, first_name, last_name, company, position
        FROM connections
        WHERE investor_id IN ({placeholders})
        AND (full_name IS NOT NULL AND full_name != '')
        """,
        tuple(investor_ids),
    ).fetchall()
    
    # Person key for deduplication
    def person_key(r):
        fn = (r["full_name"] or "").strip()
        if not fn:
            fn = f"{(r['first_name'] or '').strip()} {(r['last_name'] or '').strip()}".strip()
        return fn.lower() if fn else None
    
    # Map person -> set of investor ids
    person_to_investors = defaultdict(set)
    person_details = {}  # person_key -> {name, company, position}
    
    # Count connections per investor
    inv_connection_counts = defaultdict(int)
    
    for r in connections:
        pk = person_key(r)
        if pk:
            person_to_investors[pk].add(r["investor_id"])
            inv_connection_counts[r["investor_id"]] += 1
            if pk not in person_details:
                person_details[pk] = {
                    "name": (r["full_name"] or f"{r['first_name'] or ''} {r['last_name'] or ''}").strip(),
                    "company": r["company"] or "",
                    "position": r["position"] or "",
                }
    
    # Build overlap matrix
    n = len(investors)
    matrix = [[0] * n for _ in range(n)]
    shared_connections = {}  # (i, j) -> list of shared people
    
    for pk, inv_set in person_to_investors.items():
        if len(inv_set) >= 2:
            inv_list = list(inv_set)
            for i in range(len(inv_list)):
                for j in range(i + 1, len(inv_list)):
                    idx_i = inv_id_to_idx[inv_list[i]]
                    idx_j = inv_id_to_idx[inv_list[j]]
                    matrix[idx_i][idx_j] += 1
                    matrix[idx_j][idx_i] += 1
                    
                    key = tuple(sorted([idx_i, idx_j]))
                    if key not in shared_connections:
                        shared_connections[key] = []
                    shared_connections[key].append(person_details[pk])
    
    # Add connection counts to investors
    for inv in investors:
        inv["connection_count"] = inv_connection_counts.get(inv["id"], 0)
    
    # Convert shared_connections keys to strings for JSON
    shared_connections_json = {}
    for (i, j), people in shared_connections.items():
        key = f"{i}-{j}"
        shared_connections_json[key] = people[:20]  # Limit to 20 for display
    
    return {
        "investors": investors,
        "matrix": matrix,
        "shared_connections": shared_connections_json,
    }
