import requests

url = "https://books.toscrape.com"
response = requests.get(url)

print(f"Status: {response.status_code}")
print(f"Encoding from headers: {response.encoding}")
print(f"Apparent encoding: {response.apparent_encoding}")
print(f"Raw content first 100 bytes: {response.content[:100]}")
print(f"Text first 100 chars: {response.text[:100]}")