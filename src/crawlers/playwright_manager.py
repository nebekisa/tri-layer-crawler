"""
Playwright browser manager with pooling support.
"""

import logging
import asyncio
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from playwright.async_api import (
    async_playwright, 
    Browser, 
    BrowserContext, 
    Page,
    Playwright
)

from src.core.config_loader import get_settings

logger = logging.getLogger(__name__)


class PlaywrightManager:
    """
    Manages Playwright browser instances with pooling.
    
    Features:
        - Singleton browser instance
        - Context pooling for isolation
        - Automatic cleanup
        - Configurable viewport and timeouts
    """
    
    _instance = None
    _playwright: Optional[Playwright] = None
    _browser: Optional[Browser] = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def start(self) -> None:
        """Start Playwright and launch browser."""
        async with self._lock:
            if self._playwright is None:
                self._playwright = await async_playwright().start()
                logger.info("Playwright started")
            
            if self._browser is None:
                settings = get_settings()
                config = settings.deep_crawler
                
                browser_type = getattr(self._playwright, config.browser_type)
                
                self._browser = await browser_type.launch(
                    headless=config.headless,
                    args=['--no-sandbox', '--disable-setuid-sandbox']
                )
                logger.info(f"Browser launched: {config.browser_type} (headless={config.headless})")
    
    async def stop(self) -> None:
        """Stop browser and Playwright."""
        async with self._lock:
            if self._browser:
                await self._browser.close()
                self._browser = None
                logger.info("Browser closed")
            
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
                logger.info("Playwright stopped")
    
    @asynccontextmanager
    async def new_context(self, **kwargs) -> BrowserContext:
        """
        Create a new browser context (isolated session).
        
        Yields:
            BrowserContext: Clean browser context
        """
        await self.start()
        
        settings = get_settings()
        config = settings.deep_crawler
        
        # Default context options
        context_options = {
            'viewport': {
                'width': config.viewport_width,
                'height': config.viewport_height
            },
            'user_agent': config.user_agent,
            'java_script_enabled': config.js_enabled,
            **kwargs
        }
        
        context = await self._browser.new_context(**context_options)
        
        # Set default timeout
        context.set_default_timeout(config.timeout)
        
        logger.debug(f"Created new browser context")
        
        try:
            yield context
        finally:
            await context.close()
            logger.debug("Browser context closed")
    
    @asynccontextmanager
    async def new_page(self, **kwargs) -> Page:
        """
        Create a new page with automatic context management.
        
        Yields:
            Page: Ready-to-use Playwright page
        """
        async with self.new_context(**kwargs) as context:
            page = await context.new_page()
            try:
                yield page
            finally:
                await page.close()
    
    async def screenshot(
        self, 
        url: str, 
        output_path: str,
        full_page: bool = True,
        wait_selector: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Capture screenshot of a URL.
        
        Args:
            url: Target URL
            output_path: Where to save screenshot
            full_page: Capture entire scrollable page
            wait_selector: Optional selector to wait for
            
        Returns:
            Dict with screenshot metadata
        """
        settings = get_settings()
        config = settings.deep_crawler
        
        async with self.new_page() as page:
            logger.info(f"Navigating to: {url}")
            
            # Navigate with wait strategy
            await page.goto(
                url, 
                wait_until=config.wait_until,
                timeout=config.timeout
            )
            
            # Wait for specific element if requested
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=config.timeout)
            
            # Scroll to bottom if configured
            if config.scroll_to_bottom:
                await self._scroll_to_bottom(page)
            
            # Take screenshot
            await page.screenshot(
                path=output_path,
                full_page=full_page,
                type='jpeg',
                quality=config.screenshot_quality
            )
            
            # Get page info
            title = await page.title()
            content = await page.content()
            
            logger.info(f"Screenshot captured: {output_path}")
            
            return {
                'url': url,
                'title': title,
                'screenshot_path': output_path,
                'content_length': len(content),
                'full_page': full_page
            }
    
    async def _scroll_to_bottom(self, page: Page) -> None:
        """
        Scroll to bottom of infinite-scroll pages.
        
        Args:
            page: Playwright page
        """
        settings = get_settings()
        config = settings.deep_crawler
        
        last_height = await page.evaluate('document.body.scrollHeight')
        scrolls = 0
        max_scrolls = 10  # Prevent infinite loops
        
        while scrolls < max_scrolls:
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await asyncio.sleep(config.scroll_pause)
            
            new_height = await page.evaluate('document.body.scrollHeight')
            if new_height == last_height:
                break
            
            last_height = new_height
            scrolls += 1
        
        logger.debug(f"Scrolled {scrolls} times, final height: {last_height}")
    
    async def extract_content(
        self,
        url: str,
        wait_selector: Optional[str] = None,
        extract_selectors: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Extract content from JavaScript-rendered page.
        
        Args:
            url: Target URL
            wait_selector: Element to wait for before extraction
            extract_selectors: Dict of {name: css_selector} to extract
            
        Returns:
            Dict with extracted content
        """
        settings = get_settings()
        config = settings.deep_crawler
        
        async with self.new_page() as page:
            logger.info(f"Extracting from: {url}")
            
            # Navigate
            await self._navigate_with_smart_wait(page, url)
            
            # Wait for specific element
            if wait_selector:
                await page.wait_for_selector(wait_selector)
            
            # Scroll if needed
            if config.scroll_to_bottom:
                await self._scroll_to_bottom(page)
            
            # Get basic info
            title = await page.title()
            
            # Extract content using selectors
            extracted = {'url': url, 'title': title}
            
            if extract_selectors:
                for name, selector in extract_selectors.items():
                    try:
                        element = await page.query_selector(selector)
                        if element:
                            extracted[name] = await element.text_content()
                        else:
                            extracted[name] = None
                    except Exception as e:
                        logger.warning(f"Failed to extract {name}: {e}")
                        extracted[name] = None
            
            # Get full page text as fallback
            if 'content' not in extracted:
                extracted['content'] = await page.evaluate(
                    '() => document.body.innerText'
                )
            
            logger.info(f"Extracted {len(extracted.get('content', ''))} chars from {url}")
            
            return extracted
    async def _navigate_with_smart_wait(self, page: Page, url: str) -> None:
        """
        Navigate with fallback wait strategies.
        
        Args:
            page: Playwright page
            url: Target URL
        """
        settings = get_settings()
        config = settings.deep_crawler
        
        wait_strategies = [
            config.wait_until,  # Primary strategy
            "domcontentloaded",  # Fallback 1 (faster)
            "load",              # Fallback 2 (standard)
        ]
        
        for strategy in wait_strategies:
            try:
                await page.goto(
                    url, 
                    wait_until=strategy,
                    timeout=config.timeout
                )
                logger.debug(f"Navigation succeeded with: {strategy}")
                return
            except Exception as e:
                logger.debug(f"Navigation failed with {strategy}: {e}")
                continue
        
        # Last resort: navigate without waiting
        await page.goto(url, wait_until="commit", timeout=config.timeout)
        logger.warning(f"Navigation used fallback 'commit' for {url}")