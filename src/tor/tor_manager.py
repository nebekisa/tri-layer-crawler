"""
Tor network manager using Stem controller.
"""

import logging
from typing import Optional
import socket
import requests
from stem import Signal
from stem.control import Controller

logger = logging.getLogger(__name__)


class TorManager:
    """
    Manage Tor connection and identity rotation.
    
    Features:
        - SOCKS5 proxy for .onion requests
        - Identity rotation (NEWNYM signal)
        - Connection verification
    """
    

    def __init__(
        self,
        socks_host: str = None,
        socks_port: int = 9050,
        control_port: int = 9051,
        password: str = "CrawlerPass2024!"
    ):
        # Auto-detect: use localhost if running locally, 'tor' if in Docker
        import os
        if socks_host is None:
            socks_host = "localhost" if not os.path.exists('/.dockerenv') else "tor"
        
        self.socks_host = socks_host
        self.socks_port = socks_port
        self.control_port = control_port
        self.password = password
        
        self.proxies = {
            'http': f'socks5h://{socks_host}:{socks_port}',
            'https': f'socks5h://{socks_host}:{socks_port}'
        }
        
        print(f"TorManager using host: {socks_host}")  # Debug
        logger.info(f"TorManager initialized: {socks_host}:{socks_port}")
    
    def verify_connection(self) -> bool:
        """Verify Tor is working with better error handling."""
        import time
        
        # Wait for Tor to be ready
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                session = self.get_session()
                response = session.get(
                    'https://check.torproject.org/api/ip',
                    timeout=30
                )
                data = response.json()
                if data.get('IsTor'):
                    logger.info(f"Tor connected. Exit node: {data.get('IP')}")
                    return True
            except Exception as e:
                if attempt < max_attempts - 1:
                    logger.debug(f"Tor connection attempt {attempt + 1} failed, retrying...")
                    time.sleep(3)
                else:
                    logger.error(f"Tor connection failed after {max_attempts} attempts: {e}")
        
        return False
    
    def rotate_identity(self) -> bool:
        """
        Request new Tor identity (new exit node).
        
        Returns:
            True if rotation successful
        """
        try:
            with Controller.from_port(
                address=self.socks_host,
                port=self.control_port
            ) as controller:
                controller.authenticate(password=self.password)
                controller.signal(Signal.NEWNYM)
                logger.info("Tor identity rotated (NEWNYM signal sent)")
                return True
        except Exception as e:
            logger.error(f"Identity rotation failed: {e}")
            return False
    
    def get_session(self) -> requests.Session:
        """Get requests session configured for Tor with remote DNS."""
        session = requests.Session()
        
        # Use socks5h (the 'h' routes DNS through Tor)
        session.proxies.update({
            'http': f'socks5h://{self.socks_host}:{self.socks_port}',
            'https': f'socks5h://{self.socks_host}:{self.socks_port}'
        })
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0'
        })
        return session
    def fetch_onion(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Fetch .onion URL with retry logic."""
        import time
        
        for attempt in range(max_retries):
            try:
                session = self.get_session()
                response = session.get(url, timeout=90)
                return response
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 30
                    logger.warning(f".onion timeout, retrying in {wait}s...")
                    time.sleep(wait)
                    self.rotate_identity()  # New circuit
                else:
                    logger.error(f".onion failed after {max_retries} attempts")
                    return None
        
    def test_onion_access(self, onion_url: str = "http://checktoroadf7.onion") -> bool:
        """Test if .onion sites are accessible."""
        try:
            session = self.get_session()
            response = session.get(onion_url, timeout=60)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f".onion test failed: {e}")
            return False