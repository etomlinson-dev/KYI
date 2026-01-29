"""
Name and token normalization for KYI recommendation pipeline.
Used for de-duplication (normalized_name) and profile building (location/industry tokens).
"""
import re
import unicodedata


def normalize_name(name: str) -> str:
    """
    Produce a stable key for name matching and dedup.
    - Lowercases
    - Strips punctuation
    - Removes middle initials (single letter between spaces, or trailing " X.")
    - Collapses spaces
    - Example: "Nicholas De Noyer" -> "nicholasdenoyer"
    """
    if not name or not isinstance(name, str):
        return ""
    s = name.strip().lower()
    # Normalize unicode (e.g. accents -> ascii)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Remove punctuation
    s = re.sub(r"[^\w\s]", "", s)
    # Remove middle initials: "a b" where b is single letter, or "x." at end
    parts = s.split()
    filtered = []
    for i, p in enumerate(parts):
        if len(p) == 1 and i > 0 and i < len(parts) - 1:
            continue  # skip single-char middle initial
        filtered.append(p)
    s = " ".join(filtered)
    # Collapse spaces
    s = re.sub(r"\s+", " ", s).strip()
    # Final key: no spaces
    return s.replace(" ", "")


def tokenize_location(location: str) -> set:
    """Extract location tokens (city, state, region) for matching. Lowercase, non-empty."""
    if not location or not isinstance(location, str):
        return set()
    s = location.strip().lower()
    if not s:
        return set()
    tokens = set()
    # Full string
    tokens.add(s)
    # Parts by comma
    for part in s.split(","):
        t = part.strip()
        if t:
            tokens.add(t)
    return tokens


def tokenize_industry(text: str) -> set:
    """Extract industry-like tokens from a string (split on /,|). Lowercase, non-empty."""
    if not text or not isinstance(text, str):
        return set()
    tokens = set()
    for part in re.split(r"[/,|]", text):
        t = part.strip().lower()
        if t and len(t) > 1:
            tokens.add(t)
    return tokens


# Firm-type keywords: company names containing these suggest investor/firm.
FIRM_TYPE_TOKENS = {
    "capital", "partners", "ventures", "venture", "equity", "fund",
    "group", "holdings", "investments", "private equity", "vc",
    "venture capital", "growth", "advisors", "advisory"
}

# Title patterns that suggest investor-like roles (lowercase).
TITLE_PATTERNS = (
    "partner", "principal", "vp", "vice president", "md", "managing director",
    "director", "investor", "associate", "analyst", "head of", "managing partner"
)


def extract_firm_type_tokens(company_name: str) -> set:
    """Return firm-type tokens that appear in company_name (lowercase)."""
    if not company_name or not isinstance(company_name, str):
        return set()
    s = company_name.strip().lower()
    return {t for t in FIRM_TYPE_TOKENS if t in s}


def matches_title_pattern(title: str) -> bool:
    """True if title contains any of the investor-like title patterns."""
    if not title or not isinstance(title, str):
        return False
    t = title.strip().lower()
    return any(p in t for p in TITLE_PATTERNS)
