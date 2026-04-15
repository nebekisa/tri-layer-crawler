"""
Advanced content extraction using readability.
"""

from readability import Document
from bs4 import BeautifulSoup
import requests


def extract_main_article(html_content: str, url: str = None) -> dict:
    """
    Extract main article content using Mozilla's readability algorithm.
    
    Returns:
        dict with title, content, excerpt, and metadata
    """
    doc = Document(html_content, url=url)
    
    # Extract title
    title = doc.title()
    
    # Extract main content HTML
    content_html = doc.summary()
    
    # Parse to plain text
    soup = BeautifulSoup(content_html, 'html.parser')
    content_text = soup.get_text(separator=' ', strip=True)
    
    # Extract excerpt/summary
    meta_desc = doc.short_title()
    
    return {
        'title': title,
        'content_html': content_html,
        'content_text': content_text,
        'excerpt': meta_desc,
    }