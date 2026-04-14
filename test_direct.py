"""
Direct test bypassing all Scrapy complexity.
"""

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime

def test_direct_extraction():
    url = "https://books.toscrape.com"
    
    # Fetch with requests
    response = requests.get(url)
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract
    title_tag = soup.find('title')
    title = title_tag.string if title_tag else "No title"
    
    body = soup.find('body')
    content = body.get_text(separator=' ', strip=True) if body else ""
    
    # Create result
    result = {
        'url': url,
        'title': str(title),
        'content': content[:500] + "...",  # Preview only
        'content_length': len(content),
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    
    # Save to JSON
    with open('test_output.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    # Print
    print(f"✓ Title: {title}")
    print(f"✓ Content length: {len(content)} chars")
    print(f"✓ First 100 chars: {content[:100]}")
    print(f"✓ Saved to test_output.json")
    
    # Verify file
    with open('test_output.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        print(f"✓ JSON verified readable: {data['title']}")

if __name__ == "__main__":
    test_direct_extraction()