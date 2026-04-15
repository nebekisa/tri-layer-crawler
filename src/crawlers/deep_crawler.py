"""
Deep web crawler using Playwright for JavaScript-rendered content.
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from src.crawlers.playwright_manager import PlaywrightManager
from src.core.config_loader import get_settings

logger = logging.getLogger(__name__)


class DeepCrawler:
    """
    Crawler for JavaScript-heavy websites using Playwright.
    
    Features:
        - Full browser automation
        - Screenshot capture
        - Infinite scroll handling
        - Authentication support
        - Custom selector extraction
    """
    
    def __init__(self):
        """Initialize deep crawler."""
        self.manager = PlaywrightManager()
        self.settings = get_settings()
        self.results: List[Dict] = []
        
        # Screenshot directory
        self.screenshot_dir = Path("data/screenshots")
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("DeepCrawler initialized")
    
    async def crawl(
        self,
        urls: List[str],
        wait_selector: Optional[str] = None,
        extract_selectors: Optional[Dict[str, str]] = None,
        take_screenshot: bool = True
    ) -> List[Dict]:
        """
        Crawl multiple URLs with Playwright.
        
        Args:
            urls: List of URLs to crawl
            wait_selector: CSS selector to wait for
            extract_selectors: Dict of field -> selector mappings
            take_screenshot: Whether to capture screenshots
            
        Returns:
            List of extracted data
        """
        logger.info(f"Starting deep crawl of {len(urls)} URLs")
        
        try:
            await self.manager.start()
            
            for url in urls:
                try:
                    result = await self._crawl_single(
                        url, 
                        wait_selector, 
                        extract_selectors,
                        take_screenshot
                    )
                    if result:
                        self.results.append(result)
                        logger.info(f"✅ {url}: {result.get('title', 'No title')[:50]}")
                except Exception as e:
                    logger.error(f"Failed to crawl {url}: {e}")
            
            logger.info(f"Deep crawl complete: {len(self.results)}/{len(urls)} successful")
            
        finally:
            await self.manager.stop()
        
        return self.results
    
    async def _crawl_single(
        self,
        url: str,
        wait_selector: Optional[str],
        extract_selectors: Optional[Dict[str, str]],
        take_screenshot: bool
    ) -> Optional[Dict]:
        """Crawl a single URL."""
        
        config = self.settings.deep_crawler
        
        async with self.manager.new_page() as page:
            # Navigate
            await page.goto(url, wait_until=config.wait_until)
            
            # Wait for selector
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=config.timeout)
            elif config.wait_for_selector:
                await page.wait_for_selector(config.wait_for_selector)
            
            # Scroll if needed
            if config.scroll_to_bottom:
                await self.manager._scroll_to_bottom(page)
            
            # Extract data
            title = await page.title()
            
            result = {
                'url': url,
                'title': title,
                'crawled_at': datetime.utcnow().isoformat() + 'Z',
                'method': 'playwright'
            }
            
            # Extract using custom selectors
            if extract_selectors:
                for field, selector in extract_selectors.items():
                    try:
                        elements = await page.query_selector_all(selector)
                        if elements:
                            texts = []
                            for el in elements[:10]:  # Limit items
                                text = await el.text_content()
                                if text:
                                    texts.append(text.strip())
                            result[field] = texts if len(texts) > 1 else texts[0] if texts else None
                    except Exception as e:
                        logger.warning(f"Selector '{selector}' failed: {e}")
            
            # Get full page text
            result['content'] = await page.evaluate('() => document.body.innerText')
            
            # Take screenshot
            if take_screenshot and config.screenshot_enabled:
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                domain = url.replace('https://', '').replace('http://', '').split('/')[0]
                filename = f"{domain}_{timestamp}.jpg"
                filepath = self.screenshot_dir / filename
                
                await page.screenshot(
                    path=str(filepath),
                    full_page=True,
                    type='jpeg',
                    quality=config.screenshot_quality
                )
                result['screenshot'] = str(filepath)
            
            return result
    
    def crawl_sync(
        self,
        urls: List[str],
        **kwargs
    ) -> List[Dict]:
        """
        Synchronous wrapper for crawl().
        
        Args:
            urls: List of URLs
            **kwargs: Passed to crawl()
            
        Returns:
            List of results
        """
        return asyncio.run(self.crawl(urls, **kwargs))