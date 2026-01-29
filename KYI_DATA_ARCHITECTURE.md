# KYI Network Intelligence Platform
## Data Architecture & Collection System

---

## Executive Summary

KYI (Know Your Investor) is a network intelligence platform that aggregates professional relationship data from multiple authoritative public sources, providing users with unprecedented visibility into investor networks, business connections, and deal flow patterns.

Unlike platforms that rely on user-generated or scraped social media data, KYI sources its intelligence exclusively from **legally mandated disclosures** and **official government filings**, ensuring data accuracy, compliance, and defensibility.

---

## Data Sources & Collection Methodology

### 1. SEC EDGAR - Securities Filings

**Source:** U.S. Securities and Exchange Commission  
**URL:** https://www.sec.gov/cgi-bin/browse-edgar  
**Cost:** Free, no authentication required

| Filing Type | Data Extracted | Use Case |
|-------------|----------------|----------|
| **Form D** | Private placement offerings, fund managers, issuer details | Identify active investors and new fund formations |
| **13F Filings** | Institutional holdings over $100M | Track major institutional investor positions |
| **Proxy Statements** | Board members, executive compensation, ownership | Map corporate governance relationships |

**Collection Method:** Automated ATOM feed parsing with real-time filing alerts

---

### 2. FEC - Federal Election Commission

**Source:** Federal Election Commission  
**URL:** https://api.open.fec.gov  
**Cost:** Free API with registration

| Data Type | Fields Captured | Use Case |
|-----------|-----------------|----------|
| **Individual Contributions** | Donor name, employer, occupation, location | Reveal professional affiliations through employment disclosures |
| **Committee Contributions** | PAC affiliations, bundled donations | Identify political network clusters |

**Key Insight:** Political donors are legally required to disclose their employer and occupation, creating a verified dataset of professional relationships that cannot be obtained elsewhere.

**Collection Method:** REST API queries across 15+ employer keyword categories including venture capital, private equity, hedge funds, asset management, and family offices.

---

### 3. Wikidata - Structured Knowledge Base

**Source:** Wikimedia Foundation  
**URL:** https://query.wikidata.org  
**Cost:** Free, no authentication required

| Data Type | Fields Captured | Use Case |
|-----------|-----------------|----------|
| **Notable Investors** | Name, known investments, board positions | Identify high-profile network nodes |
| **Business Executives** | Company affiliations, positions held | Map C-suite relationship networks |
| **Board Members** | Directorship history, overlapping boards | Discover shared governance connections |

**Collection Method:** SPARQL queries targeting individuals classified as investors, venture capitalists, or business executives with documented professional relationships.

---

### 4. News & Media Intelligence

**Sources:** TechCrunch, VentureBeat, Crunchbase News  
**Cost:** Free RSS feeds

| Data Type | Fields Captured | Use Case |
|-----------|-----------------|----------|
| **Funding Announcements** | Companies, investors, round details | Track real-time deal flow |
| **Executive Moves** | Hiring, promotions, departures | Monitor network evolution |
| **Partnership News** | Strategic relationships, acquisitions | Identify emerging connections |

**Collection Method:** RSS feed aggregation with keyword filtering for funding-related content.

---

## Technical Architecture

```
+------------------+     +------------------+     +------------------+
|   SEC EDGAR      |     |   FEC API        |     |   Wikidata       |
|   (ATOM Feeds)   |     |   (REST)         |     |   (SPARQL)       |
+--------+---------+     +--------+---------+     +--------+---------+
         |                        |                        |
         v                        v                        v
+------------------------------------------------------------------------+
|                        DATA COLLECTOR                                   |
|   - Rate-limited API calls                                             |
|   - Deduplication engine                                               |
|   - Entity resolution                                                  |
|   - Relationship extraction                                            |
+------------------------------------------------------------------------+
         |
         v
+------------------------------------------------------------------------+
|                     UNIFIED DATA LAYER                                  |
|   - kyi_nodes.csv (People, Organizations)                              |
|   - kyi_edges.csv (Relationships: employment, investment, board seats) |
+------------------------------------------------------------------------+
         |
         v
+------------------------------------------------------------------------+
|                        KYI DATABASE                                     |
|   - SQLite / PostgreSQL                                                |
|   - network_nodes table                                                |
|   - network_edges table                                                |
|   - Full-text search indexes                                           |
+------------------------------------------------------------------------+
         |
         v
+------------------------------------------------------------------------+
|                        KYI WEB APPLICATION                              |
|   - Flask API Backend                                                  |
|   - D3.js Interactive Visualization                                    |
|   - Real-time Network Explorer                                         |
+------------------------------------------------------------------------+
```

---

## User Interface Integration

### Network Explorer - Solar System Visualization

The collected data powers an interactive network visualization that allows users to explore professional relationships intuitively.

#### Key Features

| Feature | Description |
|---------|-------------|
| **Infinite Zoom Navigation** | Click any node to zoom in and reveal their connections, then continue drilling down through the network |
| **Multi-Ring Orbital Layout** | Connections are displayed in concentric rings around the selected person, organized by relationship strength |
| **Overlap Detection** | Identify shared connections between multiple investors to find warm introduction paths |
| **Fullscreen Mode** | Expand to full viewport for presentations and deep analysis |
| **Connection Panel** | View complete connection lists with relationship details |

#### Visual Design

- **Center Node:** Currently selected person or entity (large, prominent)
- **Orbital Nodes:** Direct connections displayed in circular orbits
- **Node Sizing:** Reflects connection count (more connected = larger node)
- **Hover Details:** Full name, title, and relationship type on hover
- **Breadcrumb Navigation:** Track your exploration path and jump back to any point

#### Interaction Flow

```
1. User enters platform
         |
         v
2. Initial view: All investors for selected company
         |
         v
3. Click investor node → Zoom animation → View their connections
         |
         v
4. Click any connection → Continue drilling deeper
         |
         v
5. Use breadcrumbs or back button to navigate history
         |
         v
6. Toggle "Overlap View" to see shared connections matrix
```

---

## Data Quality & Compliance

### Why These Sources?

| Consideration | Our Approach |
|---------------|--------------|
| **Legal Compliance** | All data sourced from public government filings and open databases—no scraping of proprietary platforms |
| **Data Accuracy** | SEC and FEC filings are legally mandated disclosures with penalties for misrepresentation |
| **Freshness** | Automated collection runs daily, capturing new filings within 24 hours |
| **Completeness** | Multiple complementary sources fill gaps—SEC for investments, FEC for employment, Wikidata for context |

### Data Not Used

To ensure compliance and defensibility, KYI explicitly **does not** collect data from:

- LinkedIn or other social networks
- Proprietary databases requiring paid access
- Web scraping of personal websites
- Email or communication interception

---

## Current Data Volume

| Metric | Count |
|--------|-------|
| **Total Nodes** | 1,190+ |
| **Total Edges** | 839+ |
| **Unique People** | 900+ |
| **Organizations** | 290+ |
| **Data Sources** | 4 |

*Data grows continuously with each collection cycle*

---

## Competitive Differentiation

| Platform | Data Source | Limitation |
|----------|-------------|------------|
| **LinkedIn** | User-generated profiles | Requires partnership; data is self-reported and often incomplete |
| **Crunchbase** | Manual research + user submissions | Paid tiers required; focuses on startups only |
| **PitchBook** | Proprietary research | $20K+ annual subscription; limited to PE/VC |
| **KYI** | Government filings + open data | Free/low-cost sources; legally verified; broad coverage |

---

## Roadmap

### Near-Term Enhancements

1. **Additional Data Sources**
   - State corporation databases (officers/directors)
   - USPTO patent filings (inventor networks)
   - International registries (UK Companies House, EU databases)

2. **Enhanced Analytics**
   - Path-finding between any two individuals
   - Network clustering and community detection
   - Relationship strength scoring

3. **Real-Time Alerts**
   - New filing notifications for tracked individuals
   - Deal flow alerts matching investment criteria
   - Board change monitoring

---

## Summary

KYI transforms publicly available government data into actionable network intelligence. By aggregating SEC filings, FEC disclosures, and structured knowledge bases, we provide:

- **Verified professional relationships** from legally mandated disclosures
- **Interactive visualization** for intuitive network exploration
- **Compliance-first architecture** using only public, authorized data sources
- **Scalable collection** that grows the network continuously

This approach delivers unique value without the legal and ethical risks associated with social media scraping or proprietary data sources.

---

*Document Version: 1.0*  
*Last Updated: January 2026*
