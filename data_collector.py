"""
KYI Data Collector - Mega Script
Collects investor/professional network data from FREE public sources.

ALL SOURCES ARE 100% FREE - NO ACCOUNTS NEEDED (except FEC key for higher limits)

Sources:
1. SEC EDGAR - Form D filings, 13F holdings (investor/fund data)
2. Wikidata - Notable investors, executives, board members
3. FEC - Political donations with employer info
4. News RSS - Funding announcements from tech publications
5. GitHub - Developer profiles, company affiliations
6. Hacker News - Tech community discussions, hiring posts
7. Reddit - Investment/startup subreddit activity
8. Mastodon - Decentralized social network profiles

Output: CSV files ready for import into KYI system
=============================================================================
"""

import requests
import csv
import json
import time
import re
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

# Try to import feedparser, install if missing
try:
    import feedparser
except ImportError:
    print("Installing feedparser...")
    os.system("pip install feedparser --user --quiet")
    import feedparser

# FEC API Key - Get yours free at https://api.open.fec.gov/developers/
# Set as environment variable or replace DEMO_KEY below
FEC_API_KEY = os.environ.get("FEC_API_KEY", "DEMO_KEY")

# Output directory
OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

# Rate limiting helper
def rate_limit(seconds=1):
    time.sleep(seconds)

# ============================================================================
# 1. SEC EDGAR - Form D Filings (Private Placements)
# ============================================================================

def collect_sec_form_d(days_back=30):
    """
    Collect Form D filings from SEC EDGAR.
    Form D reveals private investment rounds - who invested in what.
    """
    print("\n" + "="*60)
    print("[DATA] Collecting SEC Form D Filings...")
    print("="*60)
    
    results = []
    
    # SEC EDGAR full-text search API
    base_url = "https://efts.sec.gov/LATEST/search-index"
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Search for Form D filings
    search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    params = {
        "action": "getcurrent",
        "type": "D",
        "company": "",
        "dateb": "",
        "owner": "include",
        "count": 100,
        "output": "atom"
    }
    
    try:
        headers = {"User-Agent": "KYI Research Tool (contact@example.com)"}
        response = requests.get(search_url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            # Parse Atom feed
            root = ET.fromstring(response.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            entries = root.findall('.//atom:entry', ns)
            print(f"Found {len(entries)} recent Form D filings")
            
            for entry in entries[:50]:  # Limit to 50 for demo
                title = entry.find('atom:title', ns)
                updated = entry.find('atom:updated', ns)
                link = entry.find('atom:link', ns)
                summary = entry.find('atom:summary', ns)
                
                if title is not None:
                    # Extract company name and filing info
                    title_text = title.text or ""
                    
                    # Parse title: "Form D - Company Name (CIK)"
                    match = re.search(r'D\s*-\s*(.+?)(?:\s*\(|$)', title_text)
                    company_name = match.group(1).strip() if match else title_text
                    
                    results.append({
                        'source': 'SEC_FORM_D',
                        'company_name': company_name,
                        'filing_type': 'Form D',
                        'date': updated.text[:10] if updated is not None else '',
                        'link': link.get('href') if link is not None else '',
                        'summary': summary.text[:200] if summary is not None and summary.text else '',
                        'relationship_type': 'investment'
                    })
            
            print(f"[OK] Processed {len(results)} Form D filings")
        else:
            print(f"[WARN] SEC API returned status {response.status_code}")
            
    except Exception as e:
        print(f"[WARN] Error collecting SEC Form D: {e}")
    
    return results


def collect_sec_13f(days_back=90):
    """
    Collect 13F filings - Institutional investor holdings.
    Shows what funds/institutions are invested in.
    """
    print("\n" + "="*60)
    print("[DATA] Collecting SEC 13F Filings (Institutional Holdings)...")
    print("="*60)
    
    results = []
    
    search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    params = {
        "action": "getcurrent",
        "type": "13F",
        "company": "",
        "dateb": "",
        "owner": "include",
        "count": 100,
        "output": "atom"
    }
    
    try:
        headers = {"User-Agent": "KYI Research Tool (contact@example.com)"}
        response = requests.get(search_url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            entries = root.findall('.//atom:entry', ns)
            print(f"Found {len(entries)} recent 13F filings")
            
            for entry in entries[:50]:
                title = entry.find('atom:title', ns)
                updated = entry.find('atom:updated', ns)
                link = entry.find('atom:link', ns)
                
                if title is not None:
                    title_text = title.text or ""
                    
                    # Extract fund name
                    match = re.search(r'13F.*?-\s*(.+?)(?:\s*\(|$)', title_text)
                    fund_name = match.group(1).strip() if match else title_text
                    
                    results.append({
                        'source': 'SEC_13F',
                        'investor_name': fund_name,
                        'filing_type': '13F-HR',
                        'date': updated.text[:10] if updated is not None else '',
                        'link': link.get('href') if link is not None else '',
                        'node_type': 'investor',
                        'relationship_type': 'institutional_holdings'
                    })
            
            print(f"[OK] Processed {len(results)} 13F filings")
        
    except Exception as e:
        print(f"[WARN] Error collecting SEC 13F: {e}")
    
    return results


# ============================================================================
# 2. Wikidata - Notable Investors and Executives
# ============================================================================

def collect_wikidata_investors():
    """
    Query Wikidata for notable investors, VCs, and business executives.
    Uses SPARQL endpoint - completely free.
    """
    print("\n" + "="*60)
    print("[WEB] Collecting Wikidata Investor/Executive Data...")
    print("="*60)
    
    results = []
    
    # SPARQL query for investors and business executives
    sparql_query = """
    SELECT DISTINCT ?person ?personLabel ?occupationLabel ?employerLabel ?countryLabel WHERE {
      {
        ?person wdt:P106 wd:Q484876.  # Occupation: venture capitalist
      } UNION {
        ?person wdt:P106 wd:Q43845.   # Occupation: businessperson
        ?person wdt:P108 ?employer.   # Has employer
        ?employer wdt:P31 wd:Q4830453. # Employer is type: business
      } UNION {
        ?person wdt:P106 wd:Q3918409. # Occupation: investor
      }
      
      OPTIONAL { ?person wdt:P106 ?occupation. }
      OPTIONAL { ?person wdt:P108 ?employer. }
      OPTIONAL { ?person wdt:P27 ?country. }
      
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    LIMIT 200
    """
    
    endpoint = "https://query.wikidata.org/sparql"
    
    try:
        headers = {
            "Accept": "application/json",
            "User-Agent": "KYI Research Tool (contact@example.com)"
        }
        
        response = requests.get(
            endpoint,
            params={"query": sparql_query, "format": "json"},
            headers=headers,
            timeout=60
        )
        
        if response.status_code == 200:
            data = response.json()
            bindings = data.get('results', {}).get('bindings', [])
            
            print(f"Found {len(bindings)} results from Wikidata")
            
            seen_people = set()
            for binding in bindings:
                person_uri = binding.get('person', {}).get('value', '')
                person_name = binding.get('personLabel', {}).get('value', '')
                
                # Deduplicate
                if person_uri in seen_people or not person_name:
                    continue
                seen_people.add(person_uri)
                
                # Skip if label is just the Q-number
                if person_name.startswith('Q') and person_name[1:].isdigit():
                    continue
                
                results.append({
                    'source': 'Wikidata',
                    'person_name': person_name,
                    'occupation': binding.get('occupationLabel', {}).get('value', ''),
                    'employer': binding.get('employerLabel', {}).get('value', ''),
                    'country': binding.get('countryLabel', {}).get('value', ''),
                    'wikidata_id': person_uri.split('/')[-1],
                    'node_type': 'person',
                    'relationship_type': 'professional'
                })
            
            print(f"[OK] Collected {len(results)} unique people from Wikidata")
        else:
            print(f"[WARN] Wikidata returned status {response.status_code}")
            
    except Exception as e:
        print(f"[WARN] Error querying Wikidata: {e}")
    
    return results


# ============================================================================
# 3. FEC - Political Donations (reveals employer relationships)
# ============================================================================

def collect_fec_donors(search_terms=None):
    """
    Collect FEC political donation data.
    Donors must disclose their employer - reveals professional relationships.
    """
    print("\n" + "="*60)
    print("[FEC] Collecting FEC Donor Data...")
    print("="*60)
    
    results = []
    
    if search_terms is None:
        # Expanded search terms for more data
        search_terms = [
            "venture capital",
            "private equity", 
            "investment",
            "hedge fund",
            "capital management",
            "asset management",
            "partners",
            "advisors",
            "holdings",
            "securities",
            "wealth management",
            "family office",
            "fund manager",
            "portfolio manager",
            "investor"
        ]
    
    base_url = "https://api.open.fec.gov/v1"
    
    for term in search_terms:
        try:
            # Search for individual contributors by employer
            search_url = f"{base_url}/schedules/schedule_a/"
            params = {
                "api_key": FEC_API_KEY,  # Use the configured API key
                "contributor_employer": term,
                "per_page": 100,  # Get more results
                "sort": "-contribution_receipt_date"
            }
            
            response = requests.get(search_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                contributions = data.get('results', [])
                
                print(f"Found {len(contributions)} donors for employer '{term}'")
                
                for contrib in contributions:
                    results.append({
                        'source': 'FEC',
                        'person_name': contrib.get('contributor_name', ''),
                        'employer': contrib.get('contributor_employer', ''),
                        'occupation': contrib.get('contributor_occupation', ''),
                        'city': contrib.get('contributor_city', ''),
                        'state': contrib.get('contributor_state', ''),
                        'date': contrib.get('contribution_receipt_date', ''),
                        'node_type': 'person',
                        'relationship_type': 'employment'
                    })
                
                rate_limit(0.5)  # Rate limit for demo key
            else:
                print(f"[WARN] FEC API returned {response.status_code}")
                
        except Exception as e:
            print(f"[WARN] Error collecting FEC data for '{term}': {e}")
    
    # Deduplicate by person name + employer
    seen = set()
    unique_results = []
    for r in results:
        key = (r['person_name'], r['employer'])
        if key not in seen:
            seen.add(key)
            unique_results.append(r)
    
    print(f"[OK] Collected {len(unique_results)} unique donor-employer relationships")
    return unique_results


# ============================================================================
# 4. News RSS Feeds - Funding Announcements
# ============================================================================

def collect_news_rss():
    """
    Collect funding announcements from tech news RSS feeds.
    """
    print("\n" + "="*60)
    print("[NEWS] Collecting News RSS Feeds...")
    print("="*60)
    
    results = []
    
    # RSS feeds focused on startup/investment news
    feeds = [
        ("TechCrunch", "https://techcrunch.com/feed/"),
        ("VentureBeat", "https://venturebeat.com/feed/"),
        ("Crunchbase News", "https://news.crunchbase.com/feed/"),
    ]
    
    # Keywords that indicate funding/investment news
    funding_keywords = [
        'raises', 'raised', 'funding', 'investment', 'series a', 'series b',
        'series c', 'seed round', 'venture', 'investor', 'invested', 'led by',
        'million', 'billion', 'valuation', 'acquisition', 'acquires', 'acquired'
    ]
    
    for feed_name, feed_url in feeds:
        try:
            print(f"Fetching {feed_name}...")
            feed = feedparser.parse(feed_url)
            
            relevant_entries = 0
            for entry in feed.entries[:30]:  # Check last 30 entries
                title = entry.get('title', '').lower()
                summary = entry.get('summary', '').lower()
                content = title + ' ' + summary
                
                # Check if it's funding-related
                if any(kw in content for kw in funding_keywords):
                    results.append({
                        'source': f'RSS_{feed_name}',
                        'title': entry.get('title', ''),
                        'link': entry.get('link', ''),
                        'published': entry.get('published', ''),
                        'summary': entry.get('summary', '')[:300] if entry.get('summary') else '',
                        'node_type': 'news',
                        'relationship_type': 'funding_announcement'
                    })
                    relevant_entries += 1
            
            print(f"  Found {relevant_entries} funding-related articles")
            
        except Exception as e:
            print(f"[WARN] Error fetching {feed_name}: {e}")
    
    print(f"[OK] Collected {len(results)} funding news items")
    return results


# ============================================================================
# 5. GitHub - Developer Networks and Organizations
# ============================================================================

def collect_github_users(search_terms=None):
    """
    Collect GitHub user data - developers, their organizations, and connections.
    Free API: 60 requests/hour unauthenticated, 5000/hour with token.
    """
    print("\n" + "="*60)
    print("[GITHUB] Collecting GitHub Developer Data...")
    print("="*60)
    
    results = []
    
    if search_terms is None:
        search_terms = [
            "venture capital",
            "investor",
            "founder",
            "startup",
            "angel investor",
            "partner"
        ]
    
    base_url = "https://api.github.com"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "KYI-Data-Collector"
    }
    
    for term in search_terms:
        try:
            # Search for users with term in bio
            search_url = f"{base_url}/search/users"
            params = {
                "q": f"{term} in:bio",
                "per_page": 30,
                "sort": "followers"
            }
            
            response = requests.get(search_url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                users = data.get('items', [])
                
                print(f"Found {len(users)} users for '{term}'")
                
                for user in users:
                    # Get user details
                    user_url = user.get('url', '')
                    if user_url:
                        try:
                            user_response = requests.get(user_url, headers=headers, timeout=10)
                            if user_response.status_code == 200:
                                user_data = user_response.json()
                                
                                results.append({
                                    'source': 'GitHub',
                                    'username': user_data.get('login', ''),
                                    'name': user_data.get('name', '') or user_data.get('login', ''),
                                    'bio': user_data.get('bio', '') or '',
                                    'company': user_data.get('company', '') or '',
                                    'location': user_data.get('location', '') or '',
                                    'followers': user_data.get('followers', 0),
                                    'following': user_data.get('following', 0),
                                    'public_repos': user_data.get('public_repos', 0),
                                    'profile_url': user_data.get('html_url', ''),
                                    'node_type': 'person',
                                    'relationship_type': 'developer'
                                })
                            rate_limit(0.5)  # Rate limit to avoid hitting limits
                        except:
                            pass
                
            elif response.status_code == 403:
                print(f"[WARN] GitHub rate limit reached, waiting...")
                rate_limit(60)
            else:
                print(f"[WARN] GitHub returned {response.status_code} for '{term}'")
                
            rate_limit(2)  # Be nice to API
            
        except Exception as e:
            print(f"[WARN] Error searching GitHub for '{term}': {e}")
    
    # Deduplicate by username
    seen = set()
    unique_results = []
    for r in results:
        if r['username'] not in seen:
            seen.add(r['username'])
            unique_results.append(r)
    
    print(f"[OK] Collected {len(unique_results)} unique GitHub users")
    return unique_results


# ============================================================================
# 6. Hacker News - Tech Community (Who's Hiring, Investors)
# ============================================================================

def collect_hackernews():
    """
    Collect data from Hacker News - tech community discussions.
    Free API, no authentication required.
    """
    print("\n" + "="*60)
    print("[HN] Collecting Hacker News Data...")
    print("="*60)
    
    results = []
    
    base_url = "https://hacker-news.firebaseio.com/v0"
    
    try:
        # Get top stories
        top_url = f"{base_url}/topstories.json"
        response = requests.get(top_url, timeout=30)
        
        if response.status_code == 200:
            story_ids = response.json()[:100]  # Get top 100
            
            print(f"Checking {len(story_ids)} top stories...")
            
            funding_keywords = ['funding', 'raised', 'investment', 'investor', 'vc', 'startup', 
                               'acquisition', 'series a', 'series b', 'seed', 'ipo', 'valuation']
            
            for story_id in story_ids[:50]:  # Check first 50
                try:
                    story_url = f"{base_url}/item/{story_id}.json"
                    story_response = requests.get(story_url, timeout=10)
                    
                    if story_response.status_code == 200:
                        story = story_response.json()
                        title = story.get('title', '').lower()
                        
                        if any(kw in title for kw in funding_keywords):
                            results.append({
                                'source': 'HackerNews',
                                'title': story.get('title', ''),
                                'url': story.get('url', ''),
                                'author': story.get('by', ''),
                                'score': story.get('score', 0),
                                'comments': story.get('descendants', 0),
                                'time': story.get('time', ''),
                                'node_type': 'news',
                                'relationship_type': 'discussion'
                            })
                    
                    rate_limit(0.1)  # Quick rate limit
                except:
                    pass
        
        # Also get "Who's Hiring" posts for job/company data
        ask_url = f"{base_url}/askstories.json"
        ask_response = requests.get(ask_url, timeout=30)
        
        if ask_response.status_code == 200:
            ask_ids = ask_response.json()[:30]
            
            for ask_id in ask_ids:
                try:
                    ask_story_url = f"{base_url}/item/{ask_id}.json"
                    ask_story_response = requests.get(ask_story_url, timeout=10)
                    
                    if ask_story_response.status_code == 200:
                        ask_story = ask_story_response.json()
                        title = ask_story.get('title', '')
                        
                        if 'hiring' in title.lower() or 'freelancer' in title.lower():
                            results.append({
                                'source': 'HackerNews_Hiring',
                                'title': title,
                                'author': ask_story.get('by', ''),
                                'score': ask_story.get('score', 0),
                                'comments': ask_story.get('descendants', 0),
                                'time': ask_story.get('time', ''),
                                'node_type': 'job_posting',
                                'relationship_type': 'hiring'
                            })
                    rate_limit(0.1)
                except:
                    pass
                    
    except Exception as e:
        print(f"[WARN] Error collecting from Hacker News: {e}")
    
    print(f"[OK] Collected {len(results)} Hacker News items")
    return results


# ============================================================================
# 7. Reddit - Investment Subreddits
# ============================================================================

def collect_reddit():
    """
    Collect data from Reddit investment/finance subreddits.
    Free API via JSON endpoints, no auth required.
    """
    print("\n" + "="*60)
    print("[REDDIT] Collecting Reddit Data...")
    print("="*60)
    
    results = []
    
    subreddits = [
        "venturecapital",
        "startups", 
        "Entrepreneur",
        "investing",
        "wallstreetbets",
        "SecurityAnalysis",
        "privateequity"
    ]
    
    headers = {
        "User-Agent": "KYI-Data-Collector/1.0"
    }
    
    for subreddit in subreddits:
        try:
            url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit=25"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                posts = data.get('data', {}).get('children', [])
                
                print(f"Found {len(posts)} posts in r/{subreddit}")
                
                for post in posts:
                    post_data = post.get('data', {})
                    
                    results.append({
                        'source': f'Reddit_{subreddit}',
                        'title': post_data.get('title', ''),
                        'author': post_data.get('author', ''),
                        'score': post_data.get('score', 0),
                        'comments': post_data.get('num_comments', 0),
                        'url': f"https://reddit.com{post_data.get('permalink', '')}",
                        'created': post_data.get('created_utc', ''),
                        'subreddit': subreddit,
                        'node_type': 'discussion',
                        'relationship_type': 'community_post'
                    })
                
                rate_limit(2)  # Reddit rate limits
            else:
                print(f"[WARN] Reddit returned {response.status_code} for r/{subreddit}")
                
        except Exception as e:
            print(f"[WARN] Error fetching r/{subreddit}: {e}")
    
    print(f"[OK] Collected {len(results)} Reddit posts")
    return results


# ============================================================================
# 8. Mastodon - Decentralized Social Network
# ============================================================================

def collect_mastodon():
    """
    Collect data from Mastodon - decentralized social network.
    Free API, no authentication required for public data.
    """
    print("\n" + "="*60)
    print("[MASTODON] Collecting Mastodon Data...")
    print("="*60)
    
    results = []
    
    # Popular Mastodon instances with tech/business users
    instances = [
        "mastodon.social",
        "techhub.social",
        "fosstodon.org"
    ]
    
    search_terms = ["investor", "founder", "startup", "venture capital", "entrepreneur"]
    
    for instance in instances:
        for term in search_terms[:2]:  # Limit searches per instance
            try:
                # Search public timeline
                url = f"https://{instance}/api/v2/search"
                params = {
                    "q": term,
                    "type": "accounts",
                    "limit": 20
                }
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    accounts = data.get('accounts', [])
                    
                    print(f"Found {len(accounts)} accounts for '{term}' on {instance}")
                    
                    for account in accounts:
                        results.append({
                            'source': f'Mastodon_{instance}',
                            'username': account.get('username', ''),
                            'display_name': account.get('display_name', ''),
                            'bio': account.get('note', '').replace('<p>', '').replace('</p>', '')[:200],
                            'followers': account.get('followers_count', 0),
                            'following': account.get('following_count', 0),
                            'profile_url': account.get('url', ''),
                            'instance': instance,
                            'node_type': 'person',
                            'relationship_type': 'social_profile'
                        })
                    
                    rate_limit(1)
                else:
                    print(f"[WARN] {instance} returned {response.status_code}")
                    
            except Exception as e:
                print(f"[WARN] Error searching {instance}: {e}")
    
    # Deduplicate by username+instance
    seen = set()
    unique_results = []
    for r in results:
        key = f"{r['username']}@{r.get('instance', '')}"
        if key not in seen:
            seen.add(key)
            unique_results.append(r)
    
    print(f"[OK] Collected {len(unique_results)} Mastodon accounts")
    return unique_results


# ============================================================================
# CSV Export Functions
# ============================================================================

def save_to_csv(data, filename, fieldnames=None):
    """Save data to CSV file."""
    if not data:
        print(f"[WARN] No data to save for {filename}")
        return
    
    filepath = OUTPUT_DIR / filename
    
    if fieldnames is None:
        fieldnames = list(data[0].keys())
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    
    print(f"[SAVE] Saved {len(data)} records to {filepath}")


def create_unified_network_csv(all_data):
    """
    Create a unified CSV suitable for import into KYI system.
    Format: from_name, from_type, to_name, to_type, relationship, source
    """
    print("\n" + "="*60)
    print("[LINK] Creating Unified Network CSV...")
    print("="*60)
    
    nodes = []
    edges = []
    
    # Process each data source
    for record in all_data:
        source = record.get('source', '')
        
        if source.startswith('SEC_FORM_D'):
            # Company received investment
            nodes.append({
                'id': record.get('company_name', ''),
                'label': record.get('company_name', ''),
                'node_type': 'organization',
                'source': source
            })
            
        elif source == 'SEC_13F':
            # Institutional investor
            nodes.append({
                'id': record.get('investor_name', ''),
                'label': record.get('investor_name', ''),
                'node_type': 'investor',
                'source': source
            })
            
        elif source == 'REMOVED':  # Placeholder for removed source
            nodes.append({
                'id': record.get('company_name', ''),
                'label': record.get('company_name', ''),
                'node_type': 'organization',
                'source': source,
                'meta': json.dumps({
                    'jurisdiction': record.get('jurisdiction', ''),
                    'status': record.get('status', '')
                })
            })
            
        elif source == 'Wikidata':
            person_name = record.get('person_name', '')
            employer = record.get('employer', '')
            
            nodes.append({
                'id': person_name,
                'label': person_name,
                'node_type': 'person',
                'source': source,
                'meta': json.dumps({
                    'occupation': record.get('occupation', ''),
                    'country': record.get('country', '')
                })
            })
            
            if employer:
                nodes.append({
                    'id': employer,
                    'label': employer,
                    'node_type': 'organization',
                    'source': source
                })
                edges.append({
                    'from_node': person_name,
                    'to_node': employer,
                    'relationship': 'employed_by',
                    'source': source
                })
                
        elif source == 'FEC':
            person_name = record.get('person_name', '')
            employer = record.get('employer', '')
            
            if person_name:
                nodes.append({
                    'id': person_name,
                    'label': person_name,
                    'node_type': 'person',
                    'source': source,
                    'meta': json.dumps({
                        'occupation': record.get('occupation', ''),
                        'location': f"{record.get('city', '')}, {record.get('state', '')}"
                    })
                })
            
            if employer:
                nodes.append({
                    'id': employer,
                    'label': employer,
                    'node_type': 'organization',
                    'source': source
                })
                
                if person_name and employer:
                    edges.append({
                        'from_node': person_name,
                        'to_node': employer,
                        'relationship': 'employed_by',
                        'source': source
                    })
        
        elif source == 'GitHub':
            name = record.get('name', '') or record.get('username', '')
            company = record.get('company', '')
            
            if name:
                nodes.append({
                    'id': name,
                    'label': name,
                    'node_type': 'person',
                    'source': source,
                    'meta': json.dumps({
                        'username': record.get('username', ''),
                        'bio': record.get('bio', '')[:100] if record.get('bio') else '',
                        'followers': record.get('followers', 0),
                        'profile_url': record.get('profile_url', '')
                    })
                })
            
            if company:
                # Clean company name (remove @ symbol if present)
                company_clean = company.lstrip('@').strip()
                if company_clean:
                    nodes.append({
                        'id': company_clean,
                        'label': company_clean,
                        'node_type': 'organization',
                        'source': source
                    })
                    
                    if name:
                        edges.append({
                            'from_node': name,
                            'to_node': company_clean,
                            'relationship': 'works_at',
                            'source': source
                        })
        
        elif source.startswith('Mastodon'):
            name = record.get('display_name', '') or record.get('username', '')
            
            if name:
                nodes.append({
                    'id': name,
                    'label': name,
                    'node_type': 'person',
                    'source': source,
                    'meta': json.dumps({
                        'username': record.get('username', ''),
                        'bio': record.get('bio', '')[:100] if record.get('bio') else '',
                        'followers': record.get('followers', 0),
                        'profile_url': record.get('profile_url', ''),
                        'instance': record.get('instance', '')
                    })
                })
        
        elif source.startswith('Reddit'):
            author = record.get('author', '')
            
            if author and author != '[deleted]':
                nodes.append({
                    'id': f"reddit_{author}",
                    'label': author,
                    'node_type': 'person',
                    'source': source,
                    'meta': json.dumps({
                        'subreddit': record.get('subreddit', ''),
                        'post_score': record.get('score', 0)
                    })
                })
        
        elif source.startswith('HackerNews'):
            author = record.get('author', '')
            
            if author:
                nodes.append({
                    'id': f"hn_{author}",
                    'label': author,
                    'node_type': 'person',
                    'source': source,
                    'meta': json.dumps({
                        'post_score': record.get('score', 0)
                    })
                })
    
    # Deduplicate nodes
    seen_nodes = {}
    for node in nodes:
        node_id = node.get('id', '')
        if node_id and node_id not in seen_nodes:
            seen_nodes[node_id] = node
    
    unique_nodes = list(seen_nodes.values())
    
    # Deduplicate edges
    seen_edges = set()
    unique_edges = []
    for edge in edges:
        key = (edge['from_node'], edge['to_node'], edge['relationship'])
        if key not in seen_edges:
            seen_edges.add(key)
            unique_edges.append(edge)
    
    # Save nodes
    save_to_csv(unique_nodes, 'kyi_nodes.csv', 
                ['id', 'label', 'node_type', 'source', 'meta'])
    
    # Save edges
    save_to_csv(unique_edges, 'kyi_edges.csv',
                ['from_node', 'to_node', 'relationship', 'source'])
    
    print(f"\n[OK] Created unified network:")
    print(f"   - {len(unique_nodes)} unique nodes")
    print(f"   - {len(unique_edges)} unique edges")
    
    return unique_nodes, unique_edges


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Run all data collectors and create unified output."""
    
    print("\n" + "="*70)
    print("[START] KYI DATA COLLECTOR - Starting data collection...")
    print("="*70)
    print(f"Output directory: {OUTPUT_DIR.absolute()}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    all_data = []
    
    # 1. SEC EDGAR
    try:
        form_d_data = collect_sec_form_d(days_back=30)
        save_to_csv(form_d_data, 'sec_form_d.csv')
        all_data.extend(form_d_data)
    except Exception as e:
        print(f"[WARN] SEC Form D collection failed: {e}")
    
    try:
        f13_data = collect_sec_13f(days_back=90)
        save_to_csv(f13_data, 'sec_13f.csv')
        all_data.extend(f13_data)
    except Exception as e:
        print(f"[WARN] SEC 13F collection failed: {e}")
    
    # 2. Wikidata
    try:
        wiki_data = collect_wikidata_investors()
        save_to_csv(wiki_data, 'wikidata_investors.csv')
        all_data.extend(wiki_data)
    except Exception as e:
        print(f"[WARN] Wikidata collection failed: {e}")
    
    # 3. FEC
    try:
        fec_data = collect_fec_donors()
        save_to_csv(fec_data, 'fec_donors.csv')
        all_data.extend(fec_data)
    except Exception as e:
        print(f"[WARN] FEC collection failed: {e}")
    
    # 4. News RSS
    try:
        news_data = collect_news_rss()
        save_to_csv(news_data, 'news_funding.csv')
        all_data.extend(news_data)
    except Exception as e:
        print(f"[WARN] News RSS collection failed: {e}")
    
    # 5. GitHub
    try:
        github_data = collect_github_users()
        save_to_csv(github_data, 'github_users.csv')
        all_data.extend(github_data)
    except Exception as e:
        print(f"[WARN] GitHub collection failed: {e}")
    
    # 6. Hacker News
    try:
        hn_data = collect_hackernews()
        save_to_csv(hn_data, 'hackernews.csv')
        all_data.extend(hn_data)
    except Exception as e:
        print(f"[WARN] Hacker News collection failed: {e}")
    
    # 7. Reddit
    try:
        reddit_data = collect_reddit()
        save_to_csv(reddit_data, 'reddit_posts.csv')
        all_data.extend(reddit_data)
    except Exception as e:
        print(f"[WARN] Reddit collection failed: {e}")
    
    # 8. Mastodon
    try:
        mastodon_data = collect_mastodon()
        save_to_csv(mastodon_data, 'mastodon_users.csv')
        all_data.extend(mastodon_data)
    except Exception as e:
        print(f"[WARN] Mastodon collection failed: {e}")
    
    # Create unified network CSVs for KYI import
    create_unified_network_csv(all_data)
    
    # Summary
    print("\n" + "="*70)
    print("[OK] DATA COLLECTION COMPLETE!")
    print("="*70)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nOutput files in: {OUTPUT_DIR.absolute()}")
    print("""
Files created:
    * sec_form_d.csv         - SEC Form D filings (private investments)
    * sec_13f.csv            - SEC 13F filings (institutional holdings)
    * wikidata_investors.csv - Notable investors from Wikidata
    * fec_donors.csv         - FEC donor employment data
    * news_funding.csv       - Recent funding news
    * github_users.csv       - GitHub developers/founders
    * hackernews.csv         - Hacker News discussions
    * reddit_posts.csv       - Reddit investment communities
    * mastodon_users.csv     - Mastodon tech profiles
  
  [LINK] kyi_nodes.csv       - Unified nodes for KYI import
  [LINK] kyi_edges.csv       - Unified edges for KYI import
    """)


if __name__ == "__main__":
    main()
