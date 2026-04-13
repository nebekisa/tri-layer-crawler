"""
Text Cleaning Utilities

Pure functions for cleaning and normalizing extracted text.
These are stateless and easily testable.
"""

import re
from typing import Optional


def clean_text(text: Optional[str], max_length: Optional[int] = None) -> str:
    """
    Clean and normalize extracted text content.
    
    Performs:
        - Ensures valid UTF-8 encoding
        - Strips leading/trailing whitespace
        - Collapses multiple whitespace characters into single spaces
        - Removes null bytes and control characters
        - Optionally truncates to max_length
    
    Args:
        text: Raw text extracted from HTML.
        max_length: Optional maximum length for truncation.
        
    Returns:
        Cleaned text string. Returns empty string if input is None.
    """
    if text is None:
        return ""
    
    # Convert to string (handles cases where we get numbers or other types)
    text = str(text)
    
    # CRITICAL: Ensure valid UTF-8
    try:
        text = text.encode('utf-8', errors='replace').decode('utf-8')
    except:
        pass
    
    # Remove null bytes (common in malformed HTML)
    text = text.replace('\x00', '')
    
    # Replace common HTML entities
    html_entities = {
        '&nbsp;': ' ',
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&#39;': "'",
        '&rsquo;': "'",
        '&lsquo;': "'",
        '&rdquo;': '"',
        '&ldquo;': '"',
        '&mdash;': '-',
        '&ndash;': '-',
    }
    for entity, replacement in html_entities.items():
        text = text.replace(entity, replacement)
    
    # Collapse all whitespace into single space
    text = re.sub(r'\s+', ' ', text)
    
    # Remove control characters but keep printable ones
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    # Truncate if max_length specified
    if max_length and len(text) > max_length:
        text = text[:max_length] + "..."
    
    return text


def extract_main_content(response) -> str:
    """
    Extract main textual content from a Scrapy Response object.
    
    Strategy:
        1. Try common content selectors first
        2. Fall back to all body text excluding scripts/styles
        3. Clean each fragment individually
    
    Args:
        response: Scrapy Response object.
        
    Returns:
        Cleaned text content.
    """
    content_selectors = [
        'article::text',
        'main::text',
        '.content::text',
        '.post-content::text',
        '.entry-content::text',
        '#content::text',
        '.page-content::text',
        '.site-content::text',
        'body::text',
    ]
    
    all_text = []
    
    for selector in content_selectors:
        texts = response.css(selector).getall()
        if texts:
            # Clean each text fragment individually before joining
            cleaned_texts = []
            for t in texts:
                if t and t.strip():
                    cleaned = clean_text(t)
                    if cleaned:
                        cleaned_texts.append(cleaned)
            
            all_text.extend(cleaned_texts)
            if all_text:
                break  # Stop at first selector that yields content
    
    # If no specialized selectors found, get all body text
    if not all_text:
        # XPath to get text nodes excluding script/style
        raw_texts = response.xpath(
            '//body//text()[not(ancestor::script) and not(ancestor::style)]'
        ).getall()
        
        # Clean each fragment
        for t in raw_texts:
            if t and t.strip():
                cleaned = clean_text(t)
                if cleaned:
                    all_text.append(cleaned)
    
    # Join with space and do final clean
    raw_content = ' '.join(all_text)
    
    return clean_text(raw_content)


def clean_filename(url: str) -> str:
    """
    Convert a URL into a safe filename.
    """
    # Remove protocol
    filename = re.sub(r'^https?://', '', url)
    # Replace invalid characters with underscore
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    return filename