"""
Phase 2: Access Map (Orbit) data builder.
- Inner ring: company investors
- Outer: connections (people) + organizations
- Edge types: direct (investor->person), second_degree (investor->org->person or shared person)
- Edge weight: 1 default; higher if person appears in multiple investors' networks
- Persist to network_nodes / network_edges
"""
import json
from .normalization import normalize_name


NODE_TYPE_INVESTOR = "investor"
NODE_TYPE_PERSON = "person"
NODE_TYPE_ORG = "org"
EDGE_TYPE_DIRECT = "direct"
EDGE_TYPE_SECOND_DEGREE = "second_degree"


def build_access_map(company_id: int, db, store: bool = True) -> dict:
    """
    Build access map for company: { nodes, edges, metrics }.
    Inner ring: investors. Outer: connections (people) + orgs.
    Edge weight: 1 + (num_investors_connected - 1) for shared people.
    If store=True, persist to network_nodes and network_edges (replace existing for company).
    """
    investors = db.execute(
        "SELECT id, full_name, firm, title FROM investors WHERE company_id = ?",
        (company_id,),
    ).fetchall()
    investor_ids = [r["id"] for r in investors]
    if not investor_ids:
        return {"nodes": [], "edges": [], "metrics": {}}

    placeholders = ",".join("?" * len(investor_ids))
    connections = db.execute(
        f"""
        SELECT id, investor_id, full_name, first_name, last_name, company, position, location
        FROM connections
        WHERE investor_id IN ({placeholders})
        """,
        tuple(investor_ids),
    ).fetchall()

    # Person key: normalized name for dedup
    def person_label(r):
        fn = (r["full_name"] or "").strip()
        if not fn:
            fn = f"{(r['first_name'] or '').strip()} {(r['last_name'] or '').strip()}".strip()
        return fn or "Unknown"

    nodes = []
    edges = []
    node_id_by_key = {}  # (type, key) -> id
    next_id = 1

    # Inner ring: investors
    for inv in investors:
        key = f"inv_{inv['id']}"
        node_id_by_key[(NODE_TYPE_INVESTOR, key)] = next_id
        nodes.append({
            "id": next_id,
            "company_id": company_id,
            "node_type": NODE_TYPE_INVESTOR,
            "label": inv["full_name"] or "",
            "meta_json": json.dumps({"firm": inv["firm"], "title": inv["title"], "investor_id": inv["id"]}),
        })
        next_id += 1

    # Count how many investors each person/org is connected to (for edge weight)
    person_investors = {}  # person_key -> set(investor_id)
    org_investors = {}     # org_key -> set(investor_id)
    for r in connections:
        label = person_label(r)
        pk = normalize_name(label) or label.lower()
        if pk not in person_investors:
            person_investors[pk] = set()
        person_investors[pk].add(r["investor_id"])
        org = (r["company"] or "").strip().lower()
        if org:
            if org not in org_investors:
                org_investors[org] = set()
            org_investors[org].add(r["investor_id"])

    # Outer: people (connections)
    for pk, inv_ids in person_investors.items():
        key = f"person_{pk}"
        if (NODE_TYPE_PERSON, key) not in node_id_by_key:
            node_id_by_key[(NODE_TYPE_PERSON, key)] = next_id
            # Get first label for this key from connections
            label = next((person_label(r) for r in connections if (normalize_name(person_label(r)) or person_label(r).lower()) == pk), pk)
            nodes.append({
                "id": next_id,
                "company_id": company_id,
                "node_type": NODE_TYPE_PERSON,
                "label": label,
                "meta_json": json.dumps({"shared_investors_count": len(inv_ids)}),
            })
            next_id += 1

    # Outer: orgs
    for org, inv_ids in org_investors.items():
        key = f"org_{org}"
        if (NODE_TYPE_ORG, key) not in node_id_by_key:
            node_id_by_key[(NODE_TYPE_ORG, key)] = next_id
            nodes.append({
                "id": next_id,
                "company_id": company_id,
                "node_type": NODE_TYPE_ORG,
                "label": org,
                "meta_json": json.dumps({"shared_investors_count": len(inv_ids)}),
            })
            next_id += 1

    # Edges: investor -> person (direct), weight = 1 + (len(inv_ids)-1) for shared
    for r in connections:
        inv_id = r["investor_id"]
        inv_key = f"inv_{inv_id}"
        from_nid = node_id_by_key.get((NODE_TYPE_INVESTOR, inv_key))
        label = person_label(r)
        pk = normalize_name(label) or label.lower()
        person_key = f"person_{pk}"
        to_nid = node_id_by_key.get((NODE_TYPE_PERSON, person_key))
        if from_nid and to_nid:
            weight = 1.0 + (len(person_investors.get(pk, set())) - 1) * 0.5  # boost for shared
            edges.append({
                "from_node_id": from_nid,
                "to_node_id": to_nid,
                "edge_type": EDGE_TYPE_DIRECT,
                "weight": min(weight, 5.0),
            })

    # Edges: person -> org (second_degree link for viz)
    for r in connections:
        org = (r["company"] or "").strip().lower()
        if not org:
            continue
        label = person_label(r)
        pk = normalize_name(label) or label.lower()
        person_key = f"person_{pk}"
        org_key = f"org_{org}"
        from_nid = node_id_by_key.get((NODE_TYPE_PERSON, person_key))
        to_nid = node_id_by_key.get((NODE_TYPE_ORG, org_key))
        if from_nid and to_nid:
            edges.append({
                "from_node_id": from_nid,
                "to_node_id": to_nid,
                "edge_type": EDGE_TYPE_SECOND_DEGREE,
                "weight": 1.0,
            })

    metrics = {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "investor_count": len(investors),
        "person_count": len(person_investors),
        "org_count": len(org_investors),
    }

    if store:
        _store_access_map(db, company_id, nodes, edges)

    return {"nodes": nodes, "edges": edges, "metrics": metrics}


def _store_access_map(db, company_id: int, nodes: list, edges: list):
    """Replace company's network_nodes and network_edges with new data. Map in-memory node id -> stored id."""
    now = __import__("datetime").datetime.utcnow().isoformat(timespec="seconds")
    db.execute("DELETE FROM network_nodes WHERE company_id = ?", (company_id,))
    db.execute("DELETE FROM network_edges WHERE company_id = ?", (company_id,))
    db.commit()
    id_map = {}  # in-memory node id -> stored id
    for n in nodes:
        db.execute(
            "INSERT INTO network_nodes (company_id, node_type, label, meta_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (n["company_id"], n["node_type"], n["label"], n.get("meta_json"), now),
        )
        id_map[n["id"]] = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    for e in edges:
        from_id = id_map.get(e["from_node_id"])
        to_id = id_map.get(e["to_node_id"])
        if from_id and to_id:
            db.execute(
                "INSERT INTO network_edges (company_id, from_node_id, to_node_id, edge_type, weight, meta_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (company_id, from_id, to_id, e["edge_type"], e["weight"], None, now),
            )
    db.commit()


def load_access_map(company_id: int, db) -> dict:
    """Load stored access map from network_nodes / network_edges."""
    nodes = db.execute(
        "SELECT id, company_id, node_type, label, meta_json FROM network_nodes WHERE company_id = ?",
        (company_id,),
    ).fetchall()
    edges = db.execute(
        "SELECT id, company_id, from_node_id, to_node_id, edge_type, weight, meta_json FROM network_edges WHERE company_id = ?",
        (company_id,),
    ).fetchall()
    return {
        "nodes": [dict(r) for r in nodes],
        "edges": [dict(r) for r in edges],
        "metrics": {
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
    }


def get_node_connections(node_id: int, company_id: int, db) -> dict:
    """
    Get a node and its immediate connections for solar system view.
    Returns: {center: node, connections: [connected_nodes], edges: [edges]}
    """
    # Get the center node
    center_row = db.execute(
        "SELECT id, company_id, node_type, label, meta_json FROM network_nodes WHERE id = ? AND company_id = ?",
        (node_id, company_id),
    ).fetchone()
    
    if not center_row:
        return {"center": None, "connections": [], "edges": []}
    
    center = dict(center_row)
    if center.get("meta_json"):
        try:
            center["meta"] = json.loads(center["meta_json"])
        except (json.JSONDecodeError, TypeError):
            center["meta"] = {}
    else:
        center["meta"] = {}
    
    # Get all edges connected to this node (in either direction)
    edges_rows = db.execute(
        """
        SELECT id, company_id, from_node_id, to_node_id, edge_type, weight, meta_json 
        FROM network_edges 
        WHERE company_id = ? AND (from_node_id = ? OR to_node_id = ?)
        """,
        (company_id, node_id, node_id),
    ).fetchall()
    
    edges = [dict(e) for e in edges_rows]
    
    # Collect all connected node IDs
    connected_ids = set()
    for e in edges:
        if e["from_node_id"] != node_id:
            connected_ids.add(e["from_node_id"])
        if e["to_node_id"] != node_id:
            connected_ids.add(e["to_node_id"])
    
    # Fetch connected nodes
    connections = []
    if connected_ids:
        placeholders = ",".join("?" * len(connected_ids))
        conn_rows = db.execute(
            f"SELECT id, company_id, node_type, label, meta_json FROM network_nodes WHERE id IN ({placeholders})",
            tuple(connected_ids),
        ).fetchall()
        for r in conn_rows:
            node = dict(r)
            if node.get("meta_json"):
                try:
                    node["meta"] = json.loads(node["meta_json"])
                except (json.JSONDecodeError, TypeError):
                    node["meta"] = {}
            else:
                node["meta"] = {}
            connections.append(node)
    
    return {
        "center": center,
        "connections": connections,
        "edges": edges,
    }


def get_all_investors_for_solar(company_id: int, db) -> list:
    """
    Get all investors for a company as starting points for solar system view.
    Returns list of investor nodes.
    """
    rows = db.execute(
        "SELECT id, company_id, node_type, label, meta_json FROM network_nodes WHERE company_id = ? AND node_type = ?",
        (company_id, NODE_TYPE_INVESTOR),
    ).fetchall()
    
    investors = []
    for r in rows:
        node = dict(r)
        if node.get("meta_json"):
            try:
                node["meta"] = json.loads(node["meta_json"])
            except (json.JSONDecodeError, TypeError):
                node["meta"] = {}
        else:
            node["meta"] = {}
        investors.append(node)
    
    return investors
