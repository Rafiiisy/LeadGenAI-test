import re
import logging
from urllib.parse import urlparse
# from unidecode import unidecode  # for character normalization

def extract_domain(url):
    """Extract domain from a given URL, removing www and trailing slashes."""
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower().replace("www.", "").strip("/")
        return domain
    except Exception as e:
        logging.warning(f"Failed to extract domain from URL '{url}': {e}")
        return None

def normalize_text(text):
    """Helper to lowercase and strip whitespace for consistent matching."""
    return text.lower().strip() if isinstance(text, str) else ""

def city_names_match(expected_city, text):
    """Check if expected city is mentioned in the given text."""
    expected = normalize_text(expected_city)
    actual = normalize_text(text)
    return expected in actual

def state_in_text(expected_state, text):
    """Check if expected state is mentioned in the given text."""
    expected = normalize_text(expected_state)
    actual = normalize_text(text)
    return expected in actual

# === New Utilities for Business Name Processing ===

def normalize_name(name):
    """Normalize name by lowercasing and removing accents."""
    if not name:
        return ""
    return unidecode(name.strip().lower())

def safe_split(text, delimiter):
    """Smart split that preserves abbreviations and handles punctuation."""
    if not text:
        return []

    if delimiter == '.':
        # Handle abbreviations like Inc. or Ltd.
        parts = []
        temp_parts = text.split('. ')
        for i, part in enumerate(temp_parts):
            if i < len(temp_parts) - 1:
                parts.append(part + '.')
            else:
                parts.append(part)
        return [p.strip() for p in parts if p.strip()]
    
    elif delimiter == ',':
        return [p.strip() for p in text.split(delimiter) if p.strip()]
    
    else:
        return [p for p in text.split(delimiter) if p.strip()]

def get_name_parts(name):
    """
    Split business name into chunks using intelligent delimiters while preserving structure.
    Returns (list_of_parts, delimiter_used)
    """
    normalized_name = normalize_name(name)
    logging.info(f"Normalized name: {normalized_name}")
    
    # Try by periods
    period_parts = safe_split(normalized_name, '.')
    if len(period_parts) > 1:
        logging.info(f"Split by periods: {period_parts}")
        return period_parts, '. '

    # Try by commas
    comma_parts = safe_split(normalized_name, ',')
    if len(comma_parts) > 1:
        logging.info(f"Split by commas: {comma_parts}")
        return comma_parts, ', '

    # Try known special separators
    for special_sep in [' - ', ' & ', ' + ', ' | ']:
        if special_sep in normalized_name:
            special_parts = [p.strip() for p in normalized_name.split(special_sep) if p.strip()]
            if len(special_parts) > 1:
                logging.info(f"Split by '{special_sep}': {special_parts}")
                return special_parts, special_sep

    # Fallback to space splitting with chunking
    space_parts = [p for p in normalized_name.split(' ') if p]
    if len(space_parts) >= 4:
        chunk_size = 2
        chunks = [' '.join(space_parts[i:i+chunk_size]) for i in range(0, len(space_parts), chunk_size)]
        if len(chunks) > 1:
            logging.info(f"Split into word chunks: {chunks}")
            return chunks, ' '

    return [normalized_name], None
