"""
Robust encoding handler for web scraping.
Handles encoding detection and normalization.
"""

import logging
from typing import Tuple
import chardet
from bs4 import UnicodeDammit

logger = logging.getLogger(__name__)


def decode_response_content(response_body: bytes, declared_encoding: str = None) -> str:
    """
    Robustly decode HTTP response body to UTF-8 string.
    
    Strategy:
        1. Try declared encoding from headers
        2. Use chardet for intelligent detection
        3. Fallback to UTF-8 with error replacement
        4. Use BeautifulSoup's UnicodeDammit as last resort
    
    Args:
        response_body: Raw bytes from HTTP response
        declared_encoding: Encoding from Content-Type header
        
    Returns:
        Properly decoded UTF-8 string
    """
    
    # Method 1: Try declared encoding
    if declared_encoding:
        try:
            return response_body.decode(declared_encoding)
        except (UnicodeDecodeError, LookupError):
            logger.debug(f"Failed to decode with declared encoding: {declared_encoding}")
    
    # Method 2: Use chardet for intelligent detection
    try:
        detected = chardet.detect(response_body)
        if detected and detected['confidence'] > 0.7:
            encoding = detected['encoding']
            logger.debug(f"Chardet detected: {encoding} (confidence: {detected['confidence']})")
            try:
                return response_body.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                pass
    except Exception as e:
        logger.debug(f"Chardet detection failed: {e}")
    
    # Method 3: Force UTF-8 with replacement
    try:
        return response_body.decode('utf-8', errors='replace')
    except:
        pass
    
    # Method 4: BeautifulSoup's UnicodeDammit (handles everything)
    dammit = UnicodeDammit(response_body)
    logger.debug(f"UnicodeDammit detected: {dammit.original_encoding}")
    return dammit.unicode_markup


def normalize_to_utf8(text: str) -> str:
    """
    Ensure text is clean UTF-8 without control characters.
    """
    if not text:
        return ""
    
    # Remove null bytes and control characters
    text = text.replace('\x00', '')
    
    # Remove other problematic control chars but keep newlines/tabs
    import re
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()